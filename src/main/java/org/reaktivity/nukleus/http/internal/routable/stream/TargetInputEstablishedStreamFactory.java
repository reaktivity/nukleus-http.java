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
import static org.reaktivity.nukleus.http.internal.routable.stream.Slab.NO_SLOT;
import static org.reaktivity.nukleus.http.internal.util.BufferUtil.limitOfBytes;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
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
    private final LongFunction<Correlation<?>> correlateEstablished;
    private final LongFunction<Correlation<?>> lookupEstablished;
    private final int maximumHeadersSize;
    private final Slab slab;

    public TargetInputEstablishedStreamFactory(
            Source source,
            Function<String, Target> supplyTarget,
            LongSupplier supplyStreamId,
            LongFunction<Correlation<?>> correlateEstablished,
            LongFunction<Correlation<?>> lookupEstablished,
            Slab slab)
    {
        this.source = source;
        this.supplyTarget = supplyTarget;
        this.supplyStreamId = supplyStreamId;
        this.correlateEstablished = correlateEstablished;
        this.lookupEstablished = lookupEstablished;
        this.slab = slab;
        this.maximumHeadersSize = slab.slotCapacity();
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
        private int slotIndex = NO_SLOT;
        private int slotOffset = 0;
        private int slotPosition;
        private boolean endDeferred;

        private long sourceId;

        private Target target;
        private long targetId;
        private long sourceCorrelationId;
        private int window;
        private int contentRemaining;
        private ClientConnectReplyState clientConnectReplyState;
        private long clientConnectCorrelationId;
        private int availableTargetWindow;
        @Override
        public String toString()
        {
            return String.format("%s[source=%s, sourceId=%016x, window=%d, targetId=%016x]",
                    getClass().getSimpleName(), source.routableName(), sourceId, window, targetId);
        }

        private TargetInputEstablishedStream()
        {
            this.streamState = this::streamBeforeBegin;
            this.throttleState = this::throttleBeforeReset;
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

        private void streamBeforeWindowsAreAligned(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case DataFW.TYPE_ID:
                deferData(buffer, index, length);
                break;
            case EndFW.TYPE_ID:
                deferEnd(buffer, index, length);
                break;
            default:
                processUnexpected(buffer, index, length);
                break;
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
            final long sourceRef = beginRO.sourceRef();
            this.clientConnectCorrelationId = beginRO.correlationId();

            final Correlation<?> correlation = lookupEstablished.apply(clientConnectCorrelationId);

            if (sourceRef == 0L && correlation != null)
            {
                this.streamState = this::streamAfterBeginOrData;
                this.decoderState = this::decodeHttpBegin;
                ensureSourceWindow(maximumHeadersSize);
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
                decode(payload.buffer(), offset, limit);
            }
        }

        private void decode(DirectBuffer buffer, int offset, int limit)
        {
            while (offset < limit)
            {
                offset = decoderState.decode(buffer, offset, limit);
            }
        }

        private void processEnd(
            DirectBuffer buffer,
            int index,
            int length)
        {
            endRO.wrap(buffer, index, index + length);
            final long streamId = endRO.streamId();
            assert streamId == sourceId;
            doEnd();
        }

        private void doEnd()
        {
            decoderState = (b, o, l) -> o;

            source.removeStream(sourceId);
            target.removeThrottle(targetId);
            slab.release(slotIndex);
        }

        private void deferData(
            DirectBuffer buffer,
            int index,
            int length)
        {
            dataRO.wrap(buffer, index, index + length);
            final OctetsFW payload = dataRO.payload();
            int offset = payload.offset();
            final int dataLength = payload.limit() - offset;
            MutableDirectBuffer store = slab.buffer(slotIndex);
            store.putBytes(slotPosition, payload.buffer(), offset, dataLength);
            slotPosition += dataLength;
            if (availableTargetWindow > 0)
            {
                processDeferredData();
            }
        }

        private void deferEnd(
            DirectBuffer buffer,
            int index,
            int length)
        {
            endRO.wrap(buffer, index, index + length);
            final long streamId = endRO.streamId();
            assert streamId == sourceId;

            endDeferred = true;
        }

        private int defragmentHttpBegin(
            final DirectBuffer payload,
            final int offset,
            final int limit)
        {
            // Beware of fragmented CRLFCRLF: up to the last three bytes could be on the slab
            MutableDirectBuffer buffer = slab.buffer(slotIndex);
            final int endOfHeadersAt = limitOfBytes(
                    buffer, Math.max(0, slotPosition-3), slotPosition,
                    payload, offset, limit, CRLFCRLF_BYTES);

            if (window < 2 && endOfHeadersAt == -1)
            {
                slab.release(slotIndex);
                source.doReset(sourceId);
            }
            else
            {
                int length = endOfHeadersAt == -1 ? limit - offset : endOfHeadersAt - offset;
                buffer.putBytes(slotPosition, payload, offset, length);
                slotPosition += length;
                if (endOfHeadersAt != -1)
                {
                    decodeCompleteHttpBegin(buffer, 0, slotPosition);
                    if (endOfHeadersAt < limit)
                    {
                        // Not all source data was consumed, delay processing it until target gives us window
                        slotPosition = slotOffset = 0;
                        int dataLength = limit - endOfHeadersAt;
                        assert dataLength <= buffer.capacity();
                        buffer.putBytes(0, payload, endOfHeadersAt, dataLength);
                        slotPosition = dataLength;
                        streamState = this::streamBeforeWindowsAreAligned;
                        throttleState = this::throttleBeforeWindowsAreAligned;
                    }
                    else
                    {
                        slab.release(slotIndex);
                    }
                }
            }
            return limit;
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
                if (slotIndex == NO_SLOT)
                {
                    source.doReset(sourceId);
                    doEnd();
                }
                else
                {
                    slotPosition = slotOffset = 0;
                    int length = limit - offset;
                    MutableDirectBuffer buffer = slab.buffer(slotIndex);
                    assert length <= buffer.capacity();
                    buffer.putBytes(0, payload, offset, length);
                    slotPosition = length;
                    decoderState = this::defragmentHttpBegin;
                }
            }
            else
            {
                decodeCompleteHttpBegin(payload, offset, endOfHeadersAt - offset);
                if (endOfHeadersAt < limit)
                {
                    // Not all source data was consumed, delay processing it until target gives us window
                    slotIndex = slab.acquire(sourceId);
                    if (slotIndex == NO_SLOT)
                    {
                        source.doReset(sourceId);
                        doEnd();
                    }
                    else
                    {
                        slotPosition = slotOffset = 0;
                        int length = limit - endOfHeadersAt;
                        MutableDirectBuffer buffer = slab.buffer(slotIndex);
                        assert length <= buffer.capacity();
                        buffer.putBytes(0, payload, endOfHeadersAt, length);
                        slotPosition = length;
                        streamState = this::streamBeforeWindowsAreAligned;
                        throttleState = this::throttleBeforeWindowsAreAligned;
                    }
                }
            }
            return limit;
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

                resolveTarget();

                target.doHttpBegin(targetId, 0L, sourceCorrelationId,
                        hs -> headers.forEach((k, v) -> hs.item(i -> i.representation((byte) 0).name(k).value(v))));
                target.setThrottle(targetId, this::handleThrottle);

                boolean upgraded = "101".equals(headers.get(":status"));
                String connectionOptions = headers.get("connection");
                if (connectionOptions != null)
                {
                    Arrays.asList(connectionOptions.toLowerCase().split(",")).stream().forEach((element) ->
                    {
                        if (element.equals("close"))
                        {
                            clientConnectReplyState.connection.persistent = false;
                        }
                    });
                }

                if (upgraded)
                {
                    clientConnectReplyState.connection.persistent = false;
                    this.decoderState = this::decodeHttpDataAfterUpgrade;
                }
                else
                {
                    this.contentRemaining = parseInt(headers.getOrDefault("content-length", "0"));
                    this.decoderState = this::decodeHttpData;
                }

                this.throttleState = this::throttleBeforeWindowOrReset;

                if (contentRemaining == 0)
                {
                    // no content
                    if (!upgraded)
                    {
                        httpResponseComplete();
                    }
                    else
                    {
                        clientConnectReplyState.releaseConnection(upgraded);
                    }
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
                httpResponseComplete();
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
            clientConnectReplyState.releaseConnection(false);
            return limit;
        }

        private void resolveTarget()
        {
            @SuppressWarnings("unchecked")
            final Correlation<ClientConnectReplyState> correlation =
                    (Correlation<ClientConnectReplyState>) correlateEstablished.apply(clientConnectCorrelationId);

            clientConnectReplyState = correlation.state();
            this.target = supplyTarget.apply(correlation.source());
            this.targetId = supplyStreamId.getAsLong();
            this.sourceCorrelationId = correlation.id();
            this.availableTargetWindow = 0;
        }

        private void httpResponseComplete()
        {
            target.doHttpEnd(targetId);
            target.removeThrottle(targetId);
            boolean persistent = clientConnectReplyState.connection.persistent;
            if (persistent)
            {
                this.streamState = this::streamAfterBeginOrData;
                this.decoderState = this::decodeHttpBegin;
                ensureSourceWindow(maximumHeadersSize);
            }
            else
            {
                this.streamState = this::streamAfterReplyOrReset;
            }
            clientConnectReplyState.releaseConnection(false);
        }

        private void handleThrottle(
            int msgTypeId,
            MutableDirectBuffer buffer,
            int index,
            int length)
        {
            throttleState.onMessage(msgTypeId, buffer, index, length);
        }

        private void throttleBeforeReset(
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

        private void throttleBeforeWindowsAreAligned(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case WindowFW.TYPE_ID:
                processWindowBeforeAlignment(buffer, index, length);
                break;
            case ResetFW.TYPE_ID:
                processReset(buffer, index, length);
                break;
            default:
                // ignore
                break;
            }
        }

        private void throttleBeforeWindowOrReset(
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

        private void processWindowBeforeAlignment(DirectBuffer buffer, int index, int length)
        {
            windowRO.wrap(buffer, index, index + length);
            int update = windowRO.update();
            availableTargetWindow += update;
            ensureSourceWindow(availableTargetWindow);
            processDeferredData();
        }

        private void processDeferredData()
        {
            int bytesDeferred = slotPosition - slotOffset;
            int writableBytes = Math.min(bytesDeferred, availableTargetWindow);
            MutableDirectBuffer data = slab.buffer(slotIndex);
            decode(data, slotOffset, slotOffset + writableBytes);
            availableTargetWindow -= writableBytes;

            // Continue slabbing incoming data until target window updates have caught up
            // with the initial window we gave to source
            slotOffset += writableBytes;
            bytesDeferred -= writableBytes;
            if (availableTargetWindow >= window && bytesDeferred == 0)
            {
                slab.release(slotIndex);
                slotIndex = NO_SLOT;
                if (endDeferred)
                {
                    doEnd();
                }
                else
                {
                    streamState = this::streamAfterBeginOrData;
                    throttleState = this::throttleBeforeWindowOrReset;
                }
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
            window += update;
            source.doWindow(sourceId, update + framing(update));
        }

        private void ensureSourceWindow(int requiredWindow)
        {
            if (requiredWindow > window)
            {
                int update = requiredWindow - window;
                doSourceWindow(update);
                window = requiredWindow;
            }
        }

        private void processReset(
            DirectBuffer buffer,
            int index,
            int length)
        {
            resetRO.wrap(buffer, index, index + length);
            slab.release(slotIndex);
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
