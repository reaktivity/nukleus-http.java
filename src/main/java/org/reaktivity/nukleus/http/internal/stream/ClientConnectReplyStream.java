package org.reaktivity.nukleus.http.internal.stream;

import static java.lang.Integer.parseInt;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.http.internal.stream.ClientStreamFactory.CRLFCRLF_BYTES;
import static org.reaktivity.nukleus.http.internal.stream.ClientStreamFactory.CRLF_BYTES;
import static org.reaktivity.nukleus.http.internal.stream.ClientStreamFactory.SEMICOLON_BYTES;
import static org.reaktivity.nukleus.http.internal.util.BufferUtil.limitOfBytes;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http.internal.stream.ConnectionPool.Connection;
import org.reaktivity.nukleus.http.internal.types.OctetsFW;
import org.reaktivity.nukleus.http.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http.internal.types.stream.WindowFW;

final class ClientConnectReplyStream implements MessageConsumer
{
    private final ClientStreamFactory factory;
    private final String connectReplyName;
    private final MessageConsumer connectReplyThrottle;

    private MessageConsumer streamState;
    private MessageConsumer throttleState;
    private DecoderState decoderState;

    private enum ResponseState
    {
        BEFORE_HEADERS, HEADERS, DATA, FINAL;
    };
    private ResponseState responseState;

    private int slotIndex = BufferPool.NO_SLOT;
    private int slotOffset = 0;
    private int slotPosition;
    private boolean endDeferred;

    private long sourceId;

    private MessageConsumer target;
    private long targetId;
    private String targetName;

    private long sourceCorrelationId;
    private int contentRemaining;
    private boolean isChunkedTransfer;
    private int chunkSizeRemaining;
    private long clientConnectCorrelationId;
    private ConnectionPool connectionPool;
    private Connection connection;

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
                getClass().getSimpleName(), connectReplyName, sourceId, sourceWindowBytes, targetId);
    }

    ClientConnectReplyStream(
            ClientStreamFactory factory,
            MessageConsumer connectReplyThrottle,
            long connectReplyId,
            String connectReplyName)
    {
        this.factory = factory;
        this.connectReplyThrottle = connectReplyThrottle;
        this.targetId = connectReplyId;
        this.connectReplyName = connectReplyName;
        this.streamState = this::handleStreamBeforeBegin;
        this.throttleState = this::handleThrottleBeforeBegin;
        this.windowHandler = this::handleWindow;
    }

    @Override
    public void accept(int msgTypeId, DirectBuffer buffer, int index, int length)
    {
        streamState.accept(msgTypeId, buffer, index, length);
    }

    private void handleStreamBeforeBegin(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        if (msgTypeId == BeginFW.TYPE_ID)
        {
            final BeginFW begin = this.factory.beginRO.wrap(buffer, index, index + length);
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
            final DataFW data = this.factory.dataRO.wrap(buffer, index, index + length);
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
            final DataFW data = this.factory.dataRO.wrap(buffer, index, index + length);
            handleDataWhenNotBuffering(data);
            break;
        case EndFW.TYPE_ID:
            final EndFW end = this.factory.endRO.wrap(buffer, index, index + length);
            handleEnd(end);
            break;
        default:
            handleUnexpected(buffer, index, length);
            break;
        }
    }

    private void handleStreamBeforeEnd(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case EndFW.TYPE_ID:
            final EndFW end = this.factory.endRO.wrap(buffer, index, index + length);
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
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case DataFW.TYPE_ID:
            final DataFW data = this.factory.dataRO.wrap(buffer, index, index + length);
            factory.writer.doWindow(connectReplyThrottle, data.streamId(), data.length(), 1);
            break;
        case EndFW.TYPE_ID:
            this.factory.endRO.wrap(buffer, index, index + length);
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
        this.factory.frameRO.wrap(buffer, index, index + length);
        long streamId = this.factory.frameRO.streamId();

        handleUnexpected(streamId);
    }

    private void handleUnexpected(
        long streamId)
    {
        factory.writer.doReset(connectReplyThrottle, streamId);

        this.streamState = this::handleStreamAfterReset;
    }

    private void handleInvalidResponse(boolean resetSource)
    {
        this.decoderState = this::decodeSkipData;
        this.streamState = this::handleStreamAfterReset;

        if (resetSource)
        {
            // Drain data from source before resetting to allow its writes to complete
            int window = factory.maximumHeadersSize;
            factory.writer.doWindow(connectReplyThrottle, sourceId, window, window);
            factory.writer.doReset(connectReplyThrottle, sourceId);
        }

        connection.persistent = false;
        doCleanup(!resetSource);
    }

    private void handleBegin(
        BeginFW begin)
    {
        this.sourceId = begin.streamId();
        final long sourceRef = begin.sourceRef();
        this.clientConnectCorrelationId = begin.correlationId();

        @SuppressWarnings("unchecked")
        final Correlation<ClientConnectReplyState> correlation =
                (Correlation<ClientConnectReplyState>) factory.correlations.get(clientConnectCorrelationId);
        connection = correlation.state().connection;
        connectionPool = correlation.state().connectionPool;
        connection.setInput(connectReplyThrottle, sourceId, clientConnectCorrelationId);
        if (sourceRef == 0L && correlation != null)
        {
            httpResponseBegin();
        }
        else
        {
            handleUnexpected(sourceId);
        }
    }

    private void handleDataWhenNotBuffering(
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

        if (responseState == ResponseState.BEFORE_HEADERS && target == null
                && factory.correlations.remove(clientConnectCorrelationId) == null)
        {
            responseState = ResponseState.FINAL;
        }

        switch (responseState)
        {
        case BEFORE_HEADERS:
        case HEADERS:
        case DATA:
            // Incomplete response
            handleInvalidResponse(false);
            break;
        case FINAL:
            connection.persistent = false;
            doCleanup(!connection.endSent);
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

    private void handleDataPayloadWhenDecodeIncomplete(
        final OctetsFW payload)
    {
        assert slotIndex == NO_SLOT;
        slotOffset = slotPosition = 0;
        slotIndex = factory.slab.acquire(sourceId);
        if (slotIndex == NO_SLOT)
        {
            // Out of slab memory
            factory.writer.doReset(connectReplyThrottle, sourceId);
            connection.persistent = false;
            doCleanup(false);
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

        if (slotPosition + payloadSize > factory.slab.slotCapacity())
        {
            alignSlotData();
        }

        MutableDirectBuffer slot = factory.slab.buffer(slotIndex);
        slot.putBytes(slotPosition, payload.buffer(), payload.offset(), payloadSize);
        slotPosition += payloadSize;
    }

    private void decodeBufferedData()
    {
        MutableDirectBuffer slot = factory.slab.buffer(slotIndex);
        int offset = decode(slot, slotOffset, slotPosition);
        slotOffset = offset;
        if (slotOffset == slotPosition)
        {
            factory.slab.release(slotIndex);
            slotIndex = NO_SLOT;
            streamState = this::handleStreamWhenNotBuffering;
            if (endDeferred)
            {
                connection.persistent = false;
                if (contentRemaining > 0)
                {
                    factory.writer.doAbort(target, targetId);
                }
                doCleanup(true);
            }
        }
    }

    private void handleEndWhenBuffering(
        DirectBuffer buffer,
        int index,
        int length)
    {
        this.factory.endRO.wrap(buffer, index, index + length);
        final long streamId = this.factory.endRO.streamId();
        assert streamId == sourceId;

        switch (responseState)
        {
        case BEFORE_HEADERS:
        case DATA:
            // Waiting for window to finish writing response to application
            endDeferred = true;
            break;
        default:
            handleEnd(this.factory.endRO);
        }
    }

    private void alignSlotData()
    {
        int dataLength = slotPosition - slotOffset;
        MutableDirectBuffer slot = factory.slab.buffer(slotIndex);
        factory.temporarySlot.putBytes(0, slot, slotOffset, dataLength);
        slot.putBytes(0, factory.temporarySlot, 0, dataLength);
        slotOffset = 0;
        slotPosition = dataLength;
    }

    private void doCleanup(boolean doEnd)
    {
        decoderState = (b, o, l) -> o;
        streamState = this::handleStreamAfterEnd;
        responseState = ResponseState.FINAL;
        factory.slab.release(slotIndex);
        slotIndex = NO_SLOT;
        connectionPool.release(connection, doEnd);
    }

    private int decodeHttpBegin(
        final DirectBuffer payload,
        final int offset,
        final int limit)
    {
        this.responseState = ResponseState.HEADERS;
        int result = limit;

        final int endOfHeadersAt = limitOfBytes(payload, offset, limit, CRLFCRLF_BYTES);
        if (endOfHeadersAt == -1)
        {
            result = offset;
            int length = limit - offset;
            if (length >= factory.maximumHeadersSize)
            {
                handleInvalidResponse(true);
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
            handleInvalidResponse(true);
        }
        else
        {
            final Map<String, String> headers = decodeHttpHeaders(start, lines);
            // TODO: replace with lightweight approach (end)

            resolveTarget();

            factory.writer.doHttpBegin(target, targetId, 0L, sourceCorrelationId,
                    hs -> headers.forEach((k, v) -> hs.item(i -> i.representation((byte) 0).name(k).value(v))));
            factory.router.setThrottle(targetName, targetId, this::handleThrottle);

            boolean upgraded = "101".equals(headers.get(":status"));
            String connectionOptions = headers.get("connection");
            if (connectionOptions != null)
            {
                Arrays.asList(connectionOptions.toLowerCase().split(",")).stream().forEach((element) ->
                {
                    if (element.equals("close"))
                    {
                        connection.persistent = false;
                    }
                });
            }

            if (upgraded)
            {
                connection.persistent = false;
                connectionPool.release(connection, false);
                this.decoderState = this::decodeHttpDataAfterUpgrade;
                throttleState = this::handleThrottleAfterBegin;
                windowHandler = this::handleWindow;
                this.responseState = ResponseState.DATA;
            }
            else if (contentRemaining > 0)
            {
                decoderState = this::decodeHttpData;
                throttleState = this::handleThrottleAfterBegin;
                windowHandler = this::handleBoundedWindow;
                this.responseState = ResponseState.DATA;

                sourceWindowBytesDeltaRemaining = Math.max(contentRemaining - content, 0);
            }
            else if (isChunkedTransfer)
            {
                decoderState = this::decodeHttpChunk;
                throttleState = this::handleThrottleAfterBegin;
                windowHandler = this::handleBoundedWindow;
                this.responseState = ResponseState.DATA;

                // 0\r\n\r\n
                sourceWindowBytesAdjustment += 5;
                sourceWindowBytesAdjustment -= content;
            }
            else
            {
                // no content
                httpResponseComplete();
                windowHandler = this::handleWindow;
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
                    handleInvalidResponse(true);
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
                    handleInvalidResponse(true);
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

        final int remainingBytes = Math.min(length, contentRemaining);
        final int targetWindow = targetWindowFrames == 0 ? 0 : targetWindowBytes;
        final int writableBytes = Math.min(targetWindow, remainingBytes);

        if (writableBytes > 0)
        {
            factory.writer.doHttpData(target, targetId, payload, offset, writableBytes);
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

        final int chunkHeaderLimit = limitOfBytes(payload, offset, limit, CRLF_BYTES);
        if (chunkHeaderLimit == -1)
        {
            result = offset;
        }
        else
        {
            final int semicolonAt = limitOfBytes(payload, offset, chunkHeaderLimit, SEMICOLON_BYTES);
            final int chunkSizeLimit = semicolonAt == -1 ? chunkHeaderLimit - 2 : semicolonAt - 1;
            final int chunkSizeLength = chunkSizeLimit - offset;

            try
            {
                final String chunkSizeHex = payload.getStringWithoutLengthUtf8(offset, chunkSizeLength);
                chunkSizeRemaining = Integer.parseInt(chunkSizeHex, 16);
            }
            catch (NumberFormatException ex)
            {
                handleInvalidResponse(true);
            }

            if (chunkSizeRemaining == 0)
            {
                httpResponseComplete();
            }
            else
            {
                sourceWindowBytesAdjustment += chunkSizeLength + CRLF_BYTES.length + CRLF_BYTES.length;
                sourceWindowBytesDeltaRemaining += chunkSizeRemaining;

                decoderState = this::decodeHttpChunkData;
                result = chunkHeaderLimit;
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
                handleInvalidResponse(true);
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

        final int remainingBytes = Math.min(length, chunkSizeRemaining);
        final int targetWindow = targetWindowFrames == 0 ? 0 : targetWindowBytes;
        final int writableBytes = Math.min(targetWindow, remainingBytes);

        if (writableBytes > 0)
        {
            factory.writer.doHttpData(target, targetId, payload, offset, writableBytes);
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
            factory.writer.doData(target, targetId, payload, offset, writableBytes);
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
        factory.writer.doHttpEnd(target, targetId);
        connectionPool.release(connection, true);
        return limit;
    }

    private void httpResponseBegin()
    {
        this.streamState = this::handleStreamWhenNotBuffering;
        this.decoderState = this::decodeHttpBegin;
        this.responseState = ResponseState.BEFORE_HEADERS;

        final int sourceWindowBytesDelta = this.factory.maximumHeadersSize - sourceWindowBytes + sourceWindowBytesAdjustment;

        sourceWindowBytes += sourceWindowBytesDelta;
        sourceWindowFrames = this.factory.maximumHeadersSize;

        // TODO: Support HTTP/1.1 Pipelined Responses (may be buffered already)
        sourceWindowBytesAdjustment = 0;
        sourceWindowFramesAdjustment = 0;

        factory.writer.doWindow(connectReplyThrottle, sourceId, sourceWindowBytesDelta, sourceWindowBytesDelta);
    }

    private void httpResponseComplete()
    {
        factory.writer.doHttpEnd(target, targetId);
        target = null;

        if (connection.persistent)
        {
            httpResponseBegin();
        }
        else
        {
            this.streamState = this::handleStreamBeforeEnd;
            this.responseState = ResponseState.FINAL;
        }

        connectionPool.release(connection, true);
    }

    private void resolveTarget()
    {
        final Correlation<?> correlation = factory.correlations.remove(clientConnectCorrelationId);
        this.targetName = correlation.source();
        this.target = factory.router.supplyTarget(targetName);
        this.targetId = factory.supplyStreamId.getAsLong();
        this.sourceCorrelationId = correlation.id();
        this.targetWindowBytes = 0;
    }

    private void handleThrottle(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        throttleState.accept(msgTypeId, buffer, index, length);
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
            final ResetFW reset = this.factory.resetRO.wrap(buffer, index, index + length);
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
            final WindowFW window = this.factory.windowRO.wrap(buffer, index, index + length);
            windowHandler.accept(window);
            break;
        case ResetFW.TYPE_ID:
            final ResetFW reset = this.factory.resetRO.wrap(buffer, index, index + length);
            handleReset(reset);
            break;
        default:
            // ignore
            break;
        }
    }

    private void handleBoundedWindow(
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

        if (sourceWindowBytesDeltaRemaining > 0)
        {
            final int sourceWindowBytesDelta =
                    Math.min(targetWindowBytes - sourceWindowBytes, sourceWindowBytesDeltaRemaining) +
                    sourceWindowBytesAdjustment;
            final int sourceWindowFramesDelta = targetWindowFramesDelta + sourceWindowFramesAdjustment;

            sourceWindowBytes += Math.max(sourceWindowBytesDelta, 0);
            sourceWindowBytesAdjustment = Math.min(sourceWindowBytesDelta, 0);

            sourceWindowFrames += Math.max(sourceWindowFramesDelta, 0);
            sourceWindowFramesAdjustment = Math.min(sourceWindowFramesDelta, 0);

            if (sourceWindowBytesDelta > 0 || sourceWindowFramesDelta > 0)
            {
                int windowUpdate = Math.max(sourceWindowBytesDelta, 0);
                factory.writer.doWindow(connectReplyThrottle, sourceId, windowUpdate, windowUpdate);
                sourceWindowBytesDeltaRemaining -= Math.max(sourceWindowBytesDelta, 0);
            }
        }
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
            int windowUpdate = Math.max(sourceWindowBytesDelta, 0);
            factory.writer.doWindow(connectReplyThrottle, sourceId, windowUpdate, windowUpdate);
        }
    }

    private void handleReset(
        ResetFW reset)
    {
        factory.slab.release(slotIndex);
        factory.writer.doReset(connectReplyThrottle, sourceId);
        connection.persistent = false;
        connectionPool.release(connection, false);
    }

    @FunctionalInterface interface DecoderState
    {
        int decode(DirectBuffer buffer, int offset, int limit);
    }

}