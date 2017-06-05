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

import java.nio.ByteBuffer;
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
import org.agrona.concurrent.UnsafeBuffer;
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
    private final MutableDirectBuffer temporarySlot;

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
        this.temporarySlot = new UnsafeBuffer(ByteBuffer.allocateDirect(slab.slotCapacity()));
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
            this.throttleState = this::throttleIgnoreWindow;
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

        private void streamWithDeferredData(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case DataFW.TYPE_ID:
                deferAndProcessDataFrame(buffer, index, length);
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

        private void streamBeforeEnd(
            int msgTypeId,
            MutableDirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
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

        private void streamAfterReset(
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

            this.streamState = this::streamAfterReset;
        }

        private void processInvalidResponse()
        {
            this.decoderState = decodeSkipData;
            this.streamState = this::streamAfterReset;
            if (slotIndex != NO_SLOT)
            {
                slab.release(slotIndex);
                slotIndex = NO_SLOT;
            }

            // Drain data from source before resetting to allow its writes to complete
            throttleState = this::throttlePropagateWindow;
            doSourceWindow(maximumHeadersSize);

            source.doReset(sourceId);
            doEnd();
        }

        private void processBegin(
            DirectBuffer buffer,
            int index,
            int length)
        {
            beginRO.wrap(buffer, index, index + length);

            this.sourceId = beginRO.streamId();
            final long sourceRef = beginRO.referenceId();
            this.clientConnectCorrelationId = beginRO.correlationId();

            final Correlation<?> correlation = lookupEstablished.apply(clientConnectCorrelationId);

            if (sourceRef == 0L && correlation != null)
            {
                this.streamState = this::streamAfterBeginOrData;
                this.decoderState = decodeHttpBegin;
                doSourceWindow(maximumHeadersSize);
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

                offset = decode(payload.buffer(), offset, limit);

                if (offset < limit)
                {
                    assert slotIndex == NO_SLOT;
                    slotOffset = slotPosition = 0;
                    slotIndex = slab.acquire(sourceId);
                    if (slotIndex == NO_SLOT)
                    {
                        // Out of slab memory
                        source.doReset(sourceId);
                        doEnd();
                    }
                    else
                    {
                        streamState = this::streamWithDeferredData;
                        deferAndProcessData(buffer, offset, limit);
                    }
                }
            }
        }

        private int decode(DirectBuffer buffer, int offset, int limit)
        {
            boolean decoderStateChanged = true;
            while (offset < limit && decoderStateChanged)
            {
                DecoderState previous = decoderState;
                offset = decoderState.decode(buffer, offset, limit);
                decoderStateChanged = previous != decoderState;
            }
            return offset;
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
            streamState = this::streamAfterEnd;

            source.removeStream(sourceId);
            target.removeThrottle(targetId);
            slab.release(slotIndex);
        }

        private void deferAndProcessDataFrame(
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
                deferAndProcessData(payload.buffer(), payload.offset(), payload.limit());
            }
        }

        private void deferAndProcessData(DirectBuffer buffer, int offset, int limit)
        {
            final int dataLength = limit - offset;
            if (slotPosition + dataLength > slab.slotCapacity())
            {
                alignSlotData();
            }
            MutableDirectBuffer slot = slab.buffer(slotIndex);
            slot.putBytes(slotPosition, buffer, offset, dataLength);
            slotPosition += dataLength;
            processDeferredData();
            if (window == 0)
            {
                // Increase source window to ensure we can receive the largest possible amount of data we can slab
                int cachedBytes = slotPosition - slotOffset;
                ensureSourceWindow(slab.slotCapacity() - cachedBytes);
                if (window == 0)
                {
                    throw new IllegalStateException("Decoder failed to detect headers or chunk too long");
                }
            }
        }

        private void processDeferredData()
        {
            MutableDirectBuffer slot = slab.buffer(slotIndex);
            int offset = decode(slot, slotOffset, slotPosition);
            slotOffset = offset;
            if (slotOffset == slotPosition)
            {
                slab.release(slotIndex);
                slotIndex = NO_SLOT;
                streamState = this::streamAfterBeginOrData;
                if (endDeferred)
                {
                    doEnd();
                }
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

        private void alignSlotData()
        {
            int dataLength = slotPosition - slotOffset;
            MutableDirectBuffer slot = slab.buffer(slotIndex);
            temporarySlot.putBytes(0, slot, slotOffset, dataLength);
            slot.putBytes(0, temporarySlot, 0, dataLength);
            slotOffset = 0;
            slotPosition = dataLength;
        }

        private DecoderState decodeHttpBegin = (payload, offset, limit) ->
        {
            int result = limit;
            final int endOfHeadersAt = limitOfBytes(payload, offset, limit, CRLFCRLF_BYTES);
            if (endOfHeadersAt == -1)
            {
                // Incomplete request, signal we can't consume the data
                result = offset;
                int length = limit - offset;
                if (length >= maximumHeadersSize)
                {
                    processInvalidResponse();
                }
            }
            else
            {
                decodeCompleteHttpBegin(payload, offset, endOfHeadersAt - offset);
                result = endOfHeadersAt;
            }
            return result;
        };

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
                processInvalidResponse();
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
                    clientConnectReplyState.releaseConnection(upgraded);
                    this.decoderState = decodeHttpDataAfterUpgrade;
                    throttleState = this::throttleForHttpDataAfterUpgrade;
                }
                else if ((contentRemaining = parseInt(headers.getOrDefault("content-length", "0"))) > 0)
                {
                    decoderState = decodeHttpData;
                    throttleState = this::throttleForHttpData;
                }
                else
                {
                    // no content
                    httpResponseComplete();
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

        private DecoderState decodeHttpData = (payload, offset, limit) ->
        {
            final int length = limit - offset;

            // TODO: consider chunks
            int writableBytes = Math.min(length, contentRemaining);
            writableBytes = Math.min(availableTargetWindow, writableBytes);

            if (writableBytes > 0)
            {
                target.doHttpData(targetId, payload, offset, writableBytes);
                availableTargetWindow -= writableBytes;
                contentRemaining -= writableBytes;
            }
            int result = offset + writableBytes;

            if (contentRemaining == 0)
            {
                httpResponseComplete();
            }

            return result;
        };

        private DecoderState decodeHttpDataAfterUpgrade = (payload, offset, limit) ->
        {
            final int length = limit - offset;
            int writableBytes = Math.min(length, availableTargetWindow);
            if (writableBytes > 0)
            {
                target.doData(targetId, payload, offset, writableBytes);
                availableTargetWindow -= writableBytes;
            }
            return offset + writableBytes;
        };

        private DecoderState decodeSkipData = (payload, offset, limit) ->
        {
            return limit;
        };

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

        private void httpResponseComplete()
        {
            target.doHttpEnd(targetId);
            target.removeThrottle(targetId);
            boolean persistent = clientConnectReplyState.connection.persistent;
            if (persistent)
            {
                this.streamState = this::streamAfterBeginOrData;
                this.decoderState = decodeHttpBegin;
                ensureSourceWindow(maximumHeadersSize);
            }
            else
            {
                this.streamState = this::streamBeforeEnd;
            }
            clientConnectReplyState.releaseConnection(false);
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

        private void handleThrottle(
            int msgTypeId,
            MutableDirectBuffer buffer,
            int index,
            int length)
        {
            throttleState.onMessage(msgTypeId, buffer, index, length);
        }

        private void throttleIgnoreWindow(
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

        private void throttleForHttpData(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case WindowFW.TYPE_ID:
                processWindowForHttpData(buffer, index, length);
                break;
            case ResetFW.TYPE_ID:
                processReset(buffer, index, length);
                break;
            default:
                // ignore
                break;
            }
        }

        private void throttleForHttpDataAfterUpgrade(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case WindowFW.TYPE_ID:
                processWindowForHttpDataAfterUpgrade(buffer, index, length);
                break;
            case ResetFW.TYPE_ID:
                processReset(buffer, index, length);
                break;
            default:
                // ignore
                break;
            }
        }

        private void throttlePropagateWindow(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case WindowFW.TYPE_ID:
                propagateWindow(buffer, index, length);
                break;
            case ResetFW.TYPE_ID:
                processReset(buffer, index, length);
                break;
            default:
                // ignore
                break;
            }
        }

        private void processWindowForHttpData(DirectBuffer buffer, int index, int length)
        {
            windowRO.wrap(buffer, index, index + length);
            int update = windowRO.update();

            availableTargetWindow += update;
            if (slotIndex != NO_SLOT)
            {
                processDeferredData();
            }
            ensureSourceWindow(Math.min(availableTargetWindow, slab.slotCapacity()));
        }

        private void processWindowForHttpDataAfterUpgrade(DirectBuffer buffer, int index, int length)
        {
            windowRO.wrap(buffer, index, index + length);
            int update = windowRO.update();
            availableTargetWindow += update;
            if (slotIndex != NO_SLOT)
            {
                processDeferredData();
            }
            if (slotIndex == NO_SLOT)
            {
                ensureSourceWindow(availableTargetWindow);
                if (window == availableTargetWindow)
                {
                    // Windows are now aligned
                    throttleState = this::throttlePropagateWindow;
                }
            }
        }

        private void propagateWindow(
            DirectBuffer buffer,
            int index,
            int length)
        {
            windowRO.wrap(buffer, index, index + length);
            int update = windowRO.update();
            availableTargetWindow += update;
            doSourceWindow(update);
        }

        private void ensureSourceWindow(int requiredWindow)
        {
            if (requiredWindow > window)
            {
                int update = requiredWindow - window;
                doSourceWindow(update);
            }
        }

        private void doSourceWindow(int update)
        {
            window += update;
            source.doWindow(sourceId, update + framing(update));
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
