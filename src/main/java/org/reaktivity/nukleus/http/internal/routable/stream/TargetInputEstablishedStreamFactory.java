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
import java.util.function.Consumer;
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
    private static final byte[] CRLF_BYTES = "\r\n".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] SEMICOLON_BYTES = ";".getBytes(StandardCharsets.US_ASCII);

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
        private int contentRemaining;
        private boolean isChunkedTransfer;
        private int chunkSizeRemaining;
        private ClientConnectReplyState clientConnectReplyState;
        private long clientConnectCorrelationId;

        private int sourceWindowBytes;
        private int targetWindowBytes;
        private int targetWindowFrames;
        private int sourceWindowBytesAdjustment;
        private int sourceWindowFrames;
        private int sourceWindowFramesAdjustment;
        private Consumer<WindowFW> windowHandler;
        private int sourceWindowBytesDeltaRemaining;

        @Override
        public String toString()
        {
            return String.format("%s[source=%s, sourceId=%016x, sourceWindowBytes=%d, targetId=%016x]",
                    getClass().getSimpleName(), source.routableName(), sourceId, sourceWindowBytes, targetId);
        }

        private TargetInputEstablishedStream()
        {
            this.streamState = this::handleStreamBeforeBegin;
            this.throttleState = this::handleThrottleBeforeBegin;
            this.windowHandler = this::handleWindow;
        }

        private void handleStream(
            int msgTypeId,
            MutableDirectBuffer buffer,
            int index,
            int length)
        {
            streamState.onMessage(msgTypeId, buffer, index, length);
        }

        private void handleStreamBeforeBegin(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            if (msgTypeId == BeginFW.TYPE_ID)
            {
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                handleBegin(begin);
            }
            else
            {
                handleUnexpected(buffer, index, length);
            }
        }

        private void handleStreamWhenBuffering(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                handleDataWhenBuffering(data);
                break;
            case EndFW.TYPE_ID:
                handleEndWhenBuffering(buffer, index, length);
                break;
            default:
                handleUnexpected(buffer, index, length);
                break;
            }
        }

        private void handleStreamWhenNotBuffering(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                handleData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                handleEnd(end);
                break;
            default:
                handleUnexpected(buffer, index, length);
                break;
            }
        }

        private void handleStreamBeforeEnd(
            int msgTypeId,
            MutableDirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                handleEnd(end);
                break;
            default:
                handleUnexpected(buffer, index, length);
                break;
            }
        }

        private void handleStreamAfterEnd(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            handleUnexpected(buffer, index, length);
        }

        private void handleStreamAfterReset(
            int msgTypeId,
            MutableDirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                source.doWindow(data.streamId(), data.length(), 1);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                source.removeStream(end.streamId());
                this.streamState = this::handleStreamAfterEnd;
                break;
            default:
                break;
            }
        }

        private void handleUnexpected(
            DirectBuffer buffer,
            int index,
            int length)
        {
            frameRO.wrap(buffer, index, index + length);
            long streamId = frameRO.streamId();

            handleUnexpected(streamId);
        }

        private void handleUnexpected(
            long streamId)
        {
            source.doReset(streamId);

            this.streamState = this::handleStreamAfterReset;
        }

        private void handleInvalidResponse()
        {
            this.decoderState = this::decodeSkipData;
            this.streamState = this::handleStreamAfterReset;

            // Drain data from source before resetting to allow its writes to complete
            source.doWindow(sourceId, maximumHeadersSize, maximumHeadersSize);

            source.doReset(sourceId);
            doCleanup();
        }

        private void handleBegin(
            BeginFW begin)
        {
            this.sourceId = begin.streamId();
            final long sourceRef = begin.sourceRef();
            this.clientConnectCorrelationId = begin.correlationId();

            final Correlation<?> correlation = lookupEstablished.apply(clientConnectCorrelationId);

            if (sourceRef == 0L && correlation != null)
            {
                httpResponseBegin();
            }
            else
            {
                handleUnexpected(sourceId);
            }
        }

        private void handleData(
            DataFW data)
        {
            sourceWindowBytes -= data.length();
            sourceWindowFrames--;

            if (sourceWindowBytes < 0 || sourceWindowFrames < 0)
            {
                handleUnexpected(data.streamId());
            }
            else
            {
                final OctetsFW payload = data.payload();
                final int limit = payload.limit();
                int offset = payload.offset();

                offset = decode(payload.buffer(), offset, limit);

                if (offset < limit)
                {
                    payload.wrap(payload.buffer(), offset, limit);
                    handleDataPayloadWhenDecodeIncomplete(payload);
                }
            }
        }

        private void handleEnd(
            EndFW end)
        {
            final long streamId = end.streamId();
            assert streamId == sourceId;
            doCleanup();
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

        private void handleDataPayloadWhenDecodeIncomplete(
            final OctetsFW payload)
        {
            assert slotIndex == NO_SLOT;
            slotOffset = slotPosition = 0;
            slotIndex = slab.acquire(sourceId);
            if (slotIndex == NO_SLOT)
            {
                // Out of slab memory
                source.doReset(sourceId);
                doCleanup();
            }
            else
            {
                streamState = this::handleStreamWhenBuffering;

                handleDataPayloadWhenBuffering(payload);
            }
        }

        private void handleDataWhenBuffering(
            DataFW data)
        {
            sourceWindowBytes -= data.length();
            sourceWindowFrames--;

            if (sourceWindowBytes < 0 || sourceWindowFrames < 0)
            {
                handleUnexpected(data.streamId());
            }
            else
            {
                handleDataPayloadWhenBuffering(data.payload());

                decodeBufferedData();
            }
        }

        private void handleDataPayloadWhenBuffering(
            final OctetsFW payload)
        {
            final int payloadSize = payload.sizeof();

            if (slotPosition + payloadSize > slab.slotCapacity())
            {
                alignSlotData();
            }

            MutableDirectBuffer slot = slab.buffer(slotIndex);
            slot.putBytes(slotPosition, payload.buffer(), payload.offset(), payloadSize);
            slotPosition += payloadSize;
        }

        private void decodeBufferedData()
        {
            MutableDirectBuffer slot = slab.buffer(slotIndex);
            int offset = decode(slot, slotOffset, slotPosition);
            slotOffset = offset;
            if (slotOffset == slotPosition)
            {
                slab.release(slotIndex);
                slotIndex = NO_SLOT;
                streamState = this::handleStreamWhenNotBuffering;
                if (endDeferred)
                {
                    doCleanup();
                }
            }
        }

        private void handleEndWhenBuffering(
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

        private void doCleanup()
        {
            decoderState = (b, o, l) -> o;
            streamState = this::handleStreamAfterEnd;

            source.removeStream(sourceId);
            if (target != null)
            {
                target.removeThrottle(targetId);
            }
            slab.release(slotIndex);
            slotIndex = NO_SLOT;
        }

        private int decodeHttpBegin(
            final DirectBuffer payload,
            final int offset,
            final int limit)
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
                    handleInvalidResponse();
                }
            }
            else
            {
                decodeCompleteHttpBegin(payload, offset, endOfHeadersAt - offset, limit - endOfHeadersAt);
                result = endOfHeadersAt;
            }

            return result;
        };

        private void decodeCompleteHttpBegin(
            final DirectBuffer payload,
            final int offset,
            final int length,
            final int content)
        {
            // TODO: replace with lightweight approach (start)
            String[] lines = payload.getStringWithoutLengthUtf8(offset, length).split("\r\n");
            String[] start = lines[0].split("\\s+");

            Pattern versionPattern = Pattern.compile("HTTP/1\\.(\\d)");
            Matcher versionMatcher = versionPattern.matcher(start[0]);
            if (!versionMatcher.matches())
            {
                handleInvalidResponse();
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
                    this.decoderState = this::decodeHttpDataAfterUpgrade;
                    throttleState = this::handleThrottleAfterBegin;
                }
                else if (contentRemaining > 0)
                {
                    decoderState = this::decodeHttpData;
                    throttleState = this::handleThrottleAfterBegin;
                    windowHandler = this::handleContentWindow;

                    sourceWindowBytesDeltaRemaining = Math.max(contentRemaining - content, 0);
                }
                else if (isChunkedTransfer)
                {
                    decoderState = this::decodeHttpChunk;
                    throttleState = this::handleThrottleAfterBegin;
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
            boolean contentLengthFound = false;
            contentRemaining = 0;
            isChunkedTransfer = false;
            for (int i = 1; i < lines.length; i++)
            {
                Matcher headerMatcher = headerPattern.matcher(lines[i]);
                if (!headerMatcher.matches())
                {
                    throw new IllegalStateException("illegal http header syntax");
                }

                String name = headerMatcher.group(1).toLowerCase();
                String value = headerMatcher.group(2);

                if ("transfer-encoding".equals(name))
                {
                    // TODO: support other transfer encodings
                    if (contentLengthFound || !"chunked".equals(value))
                    {
                        handleInvalidResponse();
                    }
                    else
                    {
                        isChunkedTransfer = true;
                        headers.put(name, value);
                    }
                }
                else if ("content-length".equals(name))
                {
                    if (contentLengthFound || isChunkedTransfer)
                    {
                        handleInvalidResponse();
                    }
                    else
                    {
                        contentRemaining = parseInt(value);
                        contentLengthFound = true;
                        headers.put(name, value);
                    }
                }
                else
                {
                    headers.put(name, value);
                }
            }

            return headers;
        }

        private int decodeHttpData(
            final DirectBuffer payload,
            final int offset,
            final int limit)
        {
            final int length = limit - offset;

            // TODO: consider chunks
            final int remainingBytes = Math.min(length, contentRemaining);
            final int targetWindow = targetWindowFrames == 0 ? 0 : targetWindowBytes;
            final int writableBytes = Math.min(targetWindow, remainingBytes);

            if (writableBytes > 0)
            {
                target.doHttpData(targetId, payload, offset, writableBytes);
                targetWindowBytes -= writableBytes;
                targetWindowFrames--;
                contentRemaining -= writableBytes;
            }

            if (contentRemaining == 0)
            {
                httpResponseComplete();
            }

            return offset + writableBytes;
        };

        private int decodeHttpChunk(
            final DirectBuffer payload,
            final int offset,
            final int limit)
        {
            int result = limit;

            final int endOfHeaderAt = limitOfBytes(payload, offset, limit, CRLF_BYTES);
            if (endOfHeaderAt == -1)
            {
                result = offset;
            }
            else
            {
                final int colonAt = limitOfBytes(payload, offset, limit, SEMICOLON_BYTES);
                final int chunkSizeLimit = colonAt == -1 ? endOfHeaderAt - 2 : colonAt - 1;
                final int chunkSizeLength = chunkSizeLimit - offset;

                try
                {
                    final String chunkSizeHex = payload.getStringWithoutLengthUtf8(offset, chunkSizeLength);
                    chunkSizeRemaining = Integer.parseInt(chunkSizeHex, 16);
                }
                catch (NumberFormatException ex)
                {
                    handleInvalidResponse();
                }

                if (chunkSizeRemaining == 0)
                {
                    httpResponseComplete();
                }
                else
                {
                    decoderState = this::decodeHttpChunkData;
                    result = endOfHeaderAt;
                }
            }

            return result;
        };

        private int decodeHttpChunkEnd(
            final DirectBuffer payload,
            final int offset,
            final int limit)
        {
            int length = limit - offset;
            int result = offset;

            if (length > 1)
            {
                if (payload.getByte(offset) != '\r'
                    || payload.getByte(offset + 1) != '\n')
                {
                    handleInvalidResponse();
                }
                else
                {
                    decoderState = this::decodeHttpChunk;
                    result = offset + 2;
                }
            }

            return result;
        };

        private int decodeHttpChunkData(
            final DirectBuffer payload,
            final int offset,
            final int limit)
        {
            final int length = limit - offset;

            // TODO: consider chunks
            final int remainingBytes = Math.min(length, chunkSizeRemaining);
            final int targetWindow = targetWindowFrames == 0 ? 0 : targetWindowBytes;
            final int writableBytes = Math.min(targetWindow, remainingBytes);

            if (writableBytes > 0)
            {
                target.doHttpData(targetId, payload, offset, writableBytes);
                targetWindowBytes -= writableBytes;
                targetWindowFrames--;
                chunkSizeRemaining -= writableBytes;
            }

            if (chunkSizeRemaining == 0)
            {
                decoderState = this::decodeHttpChunkEnd;
            }

            return offset + writableBytes;
        }

        private int decodeHttpDataAfterUpgrade(
                final DirectBuffer payload,
                final int offset,
                final int limit)
        {
            final int length = limit - offset;

            final int targetWindow = targetWindowFrames == 0 ? 0 : targetWindowBytes;
            final int writableBytes = Math.min(targetWindow, length);

            if (writableBytes > 0)
            {
                target.doData(targetId, payload, offset, writableBytes);
                targetWindowBytes -= writableBytes;
                targetWindowFrames--;
            }

            return offset + writableBytes;
        };

        private int decodeSkipData(
            final DirectBuffer payload,
            final int offset,
            final int limit)
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

        private void httpResponseBegin()
        {
            this.streamState = this::handleStreamWhenNotBuffering;
            this.decoderState = this::decodeHttpBegin;

            final int sourceWindowDelta = Math.max(maximumHeadersSize - sourceWindowBytes, 0);

            sourceWindowBytes += sourceWindowDelta;
            sourceWindowFrames += sourceWindowDelta;

            source.doWindow(sourceId, sourceWindowDelta, sourceWindowDelta);
        }

        private void httpResponseComplete()
        {
            target.doHttpEnd(targetId);
            target.removeThrottle(targetId);

            if (clientConnectReplyState.connection.persistent)
            {
                httpResponseBegin();
            }
            else
            {
                this.streamState = this::handleStreamBeforeEnd;
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
            this.targetWindowBytes = 0;
        }

        private void handleThrottle(
            int msgTypeId,
            MutableDirectBuffer buffer,
            int index,
            int length)
        {
            throttleState.onMessage(msgTypeId, buffer, index, length);
        }

        private void handleThrottleBeforeBegin(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                handleReset(reset);
                break;
            default:
                // ignore
                break;
            }
        }

        private void handleThrottleAfterBegin(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                windowHandler.accept(window);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                handleReset(reset);
                break;
            default:
                // ignore
                break;
            }
        }

        private void handleContentWindow(
            WindowFW window)
        {
            sourceWindowBytesDeltaRemaining -= window.update();
            sourceWindowBytesAdjustment += Math.min(sourceWindowBytesDeltaRemaining, 0);

            handleWindow(window);
        }

        private void handleWindow(
            WindowFW window)
        {
            final int targetWindowBytesDelta = window.update();
            final int targetWindowFramesDelta = window.frames();

            targetWindowBytes += targetWindowBytesDelta;
            targetWindowFrames += targetWindowFramesDelta;

            if (slotIndex != NO_SLOT)
            {
                decodeBufferedData();
            }

            final int sourceWindowBytesDelta = targetWindowBytesDelta + sourceWindowBytesAdjustment;
            final int sourceWindowFramesDelta = targetWindowFramesDelta + sourceWindowFramesAdjustment;

            sourceWindowBytes += Math.max(sourceWindowBytesDelta, 0);
            sourceWindowBytesAdjustment = Math.min(sourceWindowBytesDelta, 0);

            sourceWindowFrames += Math.max(sourceWindowFramesDelta, 0);
            sourceWindowFramesAdjustment = Math.min(sourceWindowFramesDelta, 0);

            if (sourceWindowBytesDelta > 0 || sourceWindowFramesDelta > 0)
            {
                source.doWindow(sourceId, Math.max(sourceWindowBytesDelta, 0), Math.max(sourceWindowFramesDelta, 0));
            }
        }

        private void handleReset(
            ResetFW reset)
        {
            slab.release(slotIndex);
            source.doReset(sourceId);
        }
    }

    @FunctionalInterface
    private interface DecoderState
    {
        int decode(DirectBuffer buffer, int offset, int limit);
    }
}
