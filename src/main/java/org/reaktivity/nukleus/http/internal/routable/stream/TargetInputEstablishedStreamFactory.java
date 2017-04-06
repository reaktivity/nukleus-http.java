/**
 * Copyright 2016-2017 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.reaktivity.nukleus.http.internal.routable.stream;

import static java.lang.Integer.parseInt;
import static org.reaktivity.nukleus.http.internal.util.BufferUtil.limitOfBytes;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.MessageHandler;
import org.reaktivity.nukleus.http.internal.routable.Correlation;
import org.reaktivity.nukleus.http.internal.routable.Source;
import org.reaktivity.nukleus.http.internal.routable.Target;
import org.reaktivity.nukleus.http.internal.types.OctetsFW;
import org.reaktivity.nukleus.http.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http.internal.types.stream.FrameFW;
import org.reaktivity.nukleus.http.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http.internal.types.stream.WindowFW;

public final class TargetInputEstablishedStreamFactory
{
    private static final byte[] CRLFCRLF_BYTES = "\r\n\r\n".getBytes(StandardCharsets.US_ASCII);

    private final FrameFW frameRO = new FrameFW();

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();

    private final Source source;
    private final Function<String, Target> supplyTarget;
    private final LongSupplier supplyStreamId;
    private final LongFunction<Correlation> correlateEstablished;
    private final int maximumHeadersSize;
    private final Slab slab;

    public TargetInputEstablishedStreamFactory(
            Source source,
            Function<String, Target> supplyTarget,
            LongSupplier supplyStreamId,
            LongFunction<Correlation> correlateEstablished,
            int maximumHeadersSize,
            int memoryForDecode)
    {
        this.source = source;
        this.supplyTarget = supplyTarget;
        this.supplyStreamId = supplyStreamId;
        this.correlateEstablished = correlateEstablished;
        this.maximumHeadersSize = maximumHeadersSize;
        this.slab = new Slab(memoryForDecode, maximumHeadersSize);
    }

    public MessageHandler newStream()
    {
        return new TargetInputEstablishedStream()::handleStream;
    }

    private final class TargetInputEstablishedStream
    {
        private MessageHandler streamState;
        private MessageHandler throttleState;
        private DecoderState decoderState;
        private int slotIndex;
        private int slotPosition;

        private long sourceId;

        private Target target;
        private long targetId;
        private long sourceCorrelationId;
        private int window;
        private int contentRemaining;
        private int sourceUpdateDeferred;

        @Override
        public String toString()
        {
            return String.format("%s[source=%s, sourceId=%016x, window=%d, targetId=%016x]",
                    getClass().getSimpleName(), source.routableName(), sourceId, window, targetId);
        }

        private TargetInputEstablishedStream()
        {
            this.streamState = this::streamBeforeBegin;
            this.throttleState = this::beforeReset;
        }

        private void handleStream(
            int msgTypeId,
            MutableDirectBuffer buffer,
            int index,
            int length)
        {
            streamState.onMessage(msgTypeId, buffer, index, length);
        }

        private void streamBeforeBegin(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            if (msgTypeId == BeginFW.TYPE_ID)
            {
                processBegin(buffer, index, length);
            }
            else
            {
                processUnexpected(buffer, index, length);
            }
        }

        private void streamAfterBeginOrData(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case DataFW.TYPE_ID:
                processData(buffer, index, length);
                break;
            case EndFW.TYPE_ID:
                processEnd(buffer, index, length);
                break;
            default:
                processUnexpected(buffer, index, length);
                break;
            }
        }

        private void streamAfterEnd(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            processUnexpected(buffer, index, length);
        }

        private void streamAfterReplyOrReset(
            int msgTypeId,
            MutableDirectBuffer buffer,
            int index,
            int length)
        {
            if (msgTypeId == DataFW.TYPE_ID)
            {
                dataRO.wrap(buffer, index, index + length);
                final long streamId = dataRO.streamId();

                source.doWindow(streamId, dataRO.length());
            }
            else if (msgTypeId == EndFW.TYPE_ID)
            {
                endRO.wrap(buffer, index, index + length);
                final long streamId = endRO.streamId();

                source.removeStream(streamId);

                this.streamState = this::streamAfterEnd;
            }
        }

        private void processUnexpected(
            DirectBuffer buffer,
            int index,
            int length)
        {
            frameRO.wrap(buffer, index, index + length);
            long streamId = frameRO.streamId();

            processUnexpected(streamId);
        }

        private void processUnexpected(
            long streamId)
        {
            source.doReset(streamId);

            this.streamState = this::streamAfterReplyOrReset;
        }

        private void processBegin(
            DirectBuffer buffer,
            int index,
            int length)
        {
            beginRO.wrap(buffer, index, index + length);

            this.sourceId = beginRO.streamId();
            final long sourceRef = beginRO.referenceId();
            final long targetCorrelationId = beginRO.correlationId();

            final Correlation correlation = correlateEstablished.apply(targetCorrelationId);

            if (sourceRef == 0L && correlation != null)
            {
                this.target = supplyTarget.apply(correlation.source());
                this.targetId = supplyStreamId.getAsLong();
                this.sourceCorrelationId = correlation.id();

                this.streamState = this::streamAfterBeginOrData;
                this.decoderState = this::decodeHttpBegin;

                this.window += maximumHeadersSize;

                // Make sure we don't advertise more window than available target window
                // once we have started the stream on the target
                this.sourceUpdateDeferred -= maximumHeadersSize;

                source.doWindow(sourceId, maximumHeadersSize);
            }
            else
            {
                processUnexpected(buffer, index, length);
            }
        }

        private void processData(
            DirectBuffer buffer,
            int index,
            int length)
        {
            dataRO.wrap(buffer, index, index + length);

            window -= dataRO.length();

            if (window < 0)
            {
                processUnexpected(buffer, index, length);
            }
            else
            {
                final OctetsFW payload = dataRO.payload();
                final int limit = payload.limit();

                int offset = payload.offset();
                while (offset < limit)
                {
                    offset = decoderState.decode(buffer, offset, limit);
                }
            }
        }

        private void processEnd(
            DirectBuffer buffer,
            int index,
            int length)
        {
            endRO.wrap(buffer, index, index + length);
            final long streamId = endRO.streamId();

            decoderState = (b, o, l) -> o;

            source.removeStream(streamId);
            target.removeThrottle(targetId);
        }

        private int defragmentHttpBegin(
            final DirectBuffer payload,
            final int offset,
            final int limit)
        {
            final int endOfHeadersAt = limitOfBytes(payload, offset, limit, CRLFCRLF_BYTES);
            if (window < 2 && endOfHeadersAt == -1)
            {
                slab.release(slotIndex);
                source.doReset(sourceId);
                return limit;
            }
            else
            {
                int length = endOfHeadersAt == -1 ? limit - offset : endOfHeadersAt - offset;
                MutableDirectBuffer buffer = slab.buffer(slotIndex);
                buffer.putBytes(slotPosition, payload, offset, length);
                slotPosition += length;
                if (endOfHeadersAt == -1)
                {
                    return limit;
                }
                else
                {
                    decodeCompleteHttpBegin(buffer, 0, slotPosition);
                    slab.release(slotIndex);
                    return endOfHeadersAt;
                }
            }
        }

        private int decodeHttpBegin(
            final DirectBuffer payload,
            final int offset,
            final int limit)
        {
            final int endOfHeadersAt = limitOfBytes(payload, offset, limit, CRLFCRLF_BYTES);
            if (endOfHeadersAt == -1)
            {
                slotIndex = slab.acquire(sourceId);
                slotPosition = 0;
                int length = limit - offset;
                MutableDirectBuffer buffer = slab.buffer(slotIndex);
                assert length <= buffer.capacity();
                buffer.putBytes(0, payload, offset, length);
                slotPosition = length;
                decoderState = this::defragmentHttpBegin;
                return limit;
            }
            else
            {
                decodeCompleteHttpBegin(payload, offset, endOfHeadersAt - offset);
                return endOfHeadersAt;
            }
        }

        private void decodeCompleteHttpBegin(
            final DirectBuffer payload,
            final int offset,
            final int length)
        {
            // TODO: replace with lightweight approach (start)
            String[] lines = payload.getStringWithoutLengthUtf8(offset, length).split("\r\n");
            String[] start = lines[0].split("\\s+");

            Pattern versionPattern = Pattern.compile("HTTP/1\\.(\\d)");
            Matcher versionMatcher = versionPattern.matcher(start[0]);
            if (!versionMatcher.matches())
            {
                source.doReset(sourceId);
            }
            else
            {
                final Map<String, String> headers = decodeHttpHeaders(start, lines);
                // TODO: replace with lightweight approach (end)

                target.doHttpBegin(targetId, 0L, sourceCorrelationId,
                        hs -> headers.forEach((k, v) -> hs.item(i -> i.name(k).value(v))));
                target.addThrottle(targetId, this::handleThrottle);

                boolean hasUpgrade = headers.containsKey("upgrade");

                // TODO: wait for 101 first
                if (hasUpgrade)
                {
                    this.decoderState = this::decodeHttpDataAfterUpgrade;
                }
                else
                {
                    this.contentRemaining = parseInt(headers.getOrDefault("content-length", "0"));
                    this.decoderState = this::decodeHttpData;
                }

                sourceUpdateDeferred += length;
                this.throttleState = this::beforeWindowOrReset;

                if (!hasUpgrade && contentRemaining == 0)
                {
                    // no content
                    target.doHttpEnd(targetId);
                }
            }
        }

        private Map<String, String> decodeHttpHeaders(
            String[] start,
            String[] lines)
        {
            Map<String, String> headers = new LinkedHashMap<>();
            headers.put(":status", start[1]);

            Pattern headerPattern = Pattern.compile("([^\\s:]+)\\s*:\\s*(.*)");
            for (int i = 1; i < lines.length; i++)
            {
                Matcher headerMatcher = headerPattern.matcher(lines[i]);
                if (!headerMatcher.matches())
                {
                    throw new IllegalStateException("illegal http header syntax");
                }

                String name = headerMatcher.group(1).toLowerCase();
                String value = headerMatcher.group(2);
                headers.put(name, value);
            }

            return headers;
        }

        private int decodeHttpData(
            DirectBuffer payload,
            int offset,
            int limit)
        {
            final int length = Math.min(limit - offset, contentRemaining);

            // TODO: consider chunks
            target.doHttpData(targetId, payload, offset, length);

            contentRemaining -= length;

            if (contentRemaining == 0)
            {
                target.doHttpEnd(targetId);

                this.throttleState = this::beforeWindowOrReset;
            }

            return offset + length;
        }

        private int decodeHttpDataAfterUpgrade(
            DirectBuffer payload,
            int offset,
            int limit)
        {
            target.doData(targetId, payload, offset, limit - offset);
            return limit;
        }

        @SuppressWarnings("unused")
        private int decodeHttpEnd(
            DirectBuffer payload,
            int offset,
            int limit)
        {
            // TODO: consider chunks, trailers
            target.doHttpEnd(targetId);
            return limit;
        }

        private void handleThrottle(
            int msgTypeId,
            MutableDirectBuffer buffer,
            int index,
            int length)
        {
            throttleState.onMessage(msgTypeId, buffer, index, length);
        }

        private void beforeReset(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case ResetFW.TYPE_ID:
                processReset(buffer, index, length);
                break;
            default:
                // ignore
                break;
            }
        }

        private void beforeWindowOrReset(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case WindowFW.TYPE_ID:
                processWindow(buffer, index, length);
                break;
            case ResetFW.TYPE_ID:
                processReset(buffer, index, length);
                break;
            default:
                // ignore
                break;
            }
        }

        private void processWindow(
            DirectBuffer buffer,
            int index,
            int length)
        {
            windowRO.wrap(buffer, index, index + length);

            int update = windowRO.update();
            doSourceWindow(update);
        }

        private void doSourceWindow(int update)
        {
            sourceUpdateDeferred += update;
            if (sourceUpdateDeferred > 0)
            {
                window += sourceUpdateDeferred;
                source.doWindow(sourceId, sourceUpdateDeferred + framing(sourceUpdateDeferred));
                sourceUpdateDeferred = 0;
            }
        }


        private void processReset(
            DirectBuffer buffer,
            int index,
            int length)
        {
            resetRO.wrap(buffer, index, index + length);

            source.doReset(sourceId);
        }
    }

    private static int framing(
        int payloadSize)
    {
        // TODO: consider chunks
        return 0;
    }

    @FunctionalInterface
    private interface DecoderState
    {
        int decode(DirectBuffer buffer, int offset, int length);
    }
}
