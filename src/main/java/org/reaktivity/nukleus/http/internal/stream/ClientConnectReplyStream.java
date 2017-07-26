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

    private MessageConsumer acceptReply;
    private long acceptReplyId;
    private String acceptReplyName;

    private long acceptCorrelationId;
    private int contentRemaining;
    private boolean isChunkedTransfer;
    private int chunkSizeRemaining;
    private ConnectionPool connectionPool;
    private Connection connection;

    private int connectReplyWindowBytes;
    private int acceptReplyWindowBytes;
    private int acceptReplyWindowFrames;
    private int connectReplyWindowBytesAdjustment;
    private int connectReplyWindowFrames;
    private int connectReplyWindowFramesAdjustment;
    private Consumer<WindowFW> windowHandler;
    private int connectReplyWindowBytesDeltaRemaining;

    @Override
    public String toString()
    {
        return String.format("%s[source=%s, sourceId=%016x, sourceWindowBytes=%d, targetId=%016x]",
                getClass().getSimpleName(), connectReplyName, sourceId, connectReplyWindowBytes, acceptReplyId);
    }

    ClientConnectReplyStream(
            ClientStreamFactory factory,
            MessageConsumer connectReplyThrottle,
            long connectReplyId,
            String connectReplyName)
    {
        this.factory = factory;
        this.connectReplyThrottle = connectReplyThrottle;
        this.acceptReplyId = connectReplyId;
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
        long connectCorrelationId = begin.correlationId();

        @SuppressWarnings("unchecked")
        final Correlation<ClientConnectReplyState> correlation =
                (Correlation<ClientConnectReplyState>) factory.correlations.get(connectCorrelationId);
        connection = correlation.state().connection;
        connectionPool = correlation.state().connectionPool;
        connection.setInput(connectReplyThrottle, sourceId);
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
        connectReplyWindowBytes -= data.length();
        connectReplyWindowFrames--;

        if (connectReplyWindowBytes < 0 || connectReplyWindowFrames < 0)
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

        if (responseState == ResponseState.BEFORE_HEADERS && acceptReply == null
                && factory.correlations.get(connection.correlationId) == null)
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
        connectReplyWindowBytes -= data.length();
        connectReplyWindowFrames--;

        if (connectReplyWindowBytes < 0 || connectReplyWindowFrames < 0)
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
                    factory.writer.doAbort(acceptReply, acceptReplyId);
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
        releaseSlotIfNecessary();
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

            factory.writer.doHttpBegin(acceptReply, acceptReplyId, 0L, acceptCorrelationId,
                    hs -> headers.forEach((k, v) -> hs.item(i -> i.representation((byte) 0).name(k).value(v))));
            factory.router.setThrottle(acceptReplyName, acceptReplyId, this::handleThrottle);

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

                connectReplyWindowBytesDeltaRemaining = Math.max(contentRemaining - content, 0);
            }
            else if (isChunkedTransfer)
            {
                decoderState = this::decodeHttpChunk;
                throttleState = this::handleThrottleAfterBegin;
                windowHandler = this::handleBoundedWindow;
                this.responseState = ResponseState.DATA;

                // 0\r\n\r\n
                connectReplyWindowBytesAdjustment += 5;
                connectReplyWindowBytesAdjustment -= content;
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
        final int targetWindow = acceptReplyWindowFrames == 0 ? 0 : acceptReplyWindowBytes;
        final int writableBytes = Math.min(targetWindow, remainingBytes);

        if (writableBytes > 0)
        {
            factory.writer.doHttpData(acceptReply, acceptReplyId, payload, offset, writableBytes);
            acceptReplyWindowBytes -= writableBytes;
            acceptReplyWindowFrames--;
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
                connectReplyWindowBytesAdjustment += chunkSizeLength + CRLF_BYTES.length + CRLF_BYTES.length;
                connectReplyWindowBytesDeltaRemaining += chunkSizeRemaining;

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
        final int targetWindow = acceptReplyWindowFrames == 0 ? 0 : acceptReplyWindowBytes;
        final int writableBytes = Math.min(targetWindow, remainingBytes);

        if (writableBytes > 0)
        {
            factory.writer.doHttpData(acceptReply, acceptReplyId, payload, offset, writableBytes);
            acceptReplyWindowBytes -= writableBytes;
            acceptReplyWindowFrames--;
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

        final int targetWindow = acceptReplyWindowFrames == 0 ? 0 : acceptReplyWindowBytes;
        final int writableBytes = Math.min(targetWindow, length);

        if (writableBytes > 0)
        {
            factory.writer.doData(acceptReply, acceptReplyId, payload, offset, writableBytes);
            acceptReplyWindowBytes -= writableBytes;
            acceptReplyWindowFrames--;
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
        factory.writer.doHttpEnd(acceptReply, acceptReplyId);
        connectionPool.release(connection, true);
        return limit;
    }

    private void httpResponseBegin()
    {
        this.streamState = this::handleStreamWhenNotBuffering;
        this.decoderState = this::decodeHttpBegin;
        this.responseState = ResponseState.BEFORE_HEADERS;

        final int connectReplyWindowBytesDelta =
                factory.maximumHeadersSize - connectReplyWindowBytes + connectReplyWindowBytesAdjustment;

        connectReplyWindowBytes += connectReplyWindowBytesDelta;
        connectReplyWindowFrames = this.factory.maximumHeadersSize;

        // TODO: Support HTTP/1.1 Pipelined Responses (may be buffered already)
        connectReplyWindowBytesAdjustment = 0;
        connectReplyWindowFramesAdjustment = 0;

        factory.writer.doWindow(connectReplyThrottle, sourceId, connectReplyWindowBytesDelta, connectReplyWindowBytesDelta);
    }

    private void httpResponseComplete()
    {
        factory.writer.doHttpEnd(acceptReply, acceptReplyId);
        acceptReply = null;

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
        final Correlation<?> correlation = factory.correlations.remove(connection.correlationId);
        this.acceptReplyName = correlation.source();
        this.acceptReply = factory.router.supplyTarget(acceptReplyName);
        this.acceptReplyId = factory.supplyStreamId.getAsLong();
        this.acceptCorrelationId = correlation.id();
        this.acceptReplyWindowBytes = 0;
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

        acceptReplyWindowBytes += targetWindowBytesDelta;
        acceptReplyWindowFrames += targetWindowFramesDelta;

        if (slotIndex != NO_SLOT)
        {
            decodeBufferedData();
        }

        if (connectReplyWindowBytesDeltaRemaining > 0)
        {
            final int sourceWindowBytesDelta =
                    Math.min(acceptReplyWindowBytes - connectReplyWindowBytes, connectReplyWindowBytesDeltaRemaining) +
                    connectReplyWindowBytesAdjustment;
            final int sourceWindowFramesDelta = targetWindowFramesDelta + connectReplyWindowFramesAdjustment;

            connectReplyWindowBytes += Math.max(sourceWindowBytesDelta, 0);
            connectReplyWindowBytesAdjustment = Math.min(sourceWindowBytesDelta, 0);

            connectReplyWindowFrames += Math.max(sourceWindowFramesDelta, 0);
            connectReplyWindowFramesAdjustment = Math.min(sourceWindowFramesDelta, 0);

            if (sourceWindowBytesDelta > 0 || sourceWindowFramesDelta > 0)
            {
                int windowUpdate = Math.max(sourceWindowBytesDelta, 0);
                factory.writer.doWindow(connectReplyThrottle, sourceId, windowUpdate, windowUpdate);
                connectReplyWindowBytesDeltaRemaining -= Math.max(sourceWindowBytesDelta, 0);
            }
        }
    }

    private void handleWindow(
        WindowFW window)
    {
        final int targetWindowBytesDelta = window.update();
        final int targetWindowFramesDelta = window.frames();

        acceptReplyWindowBytes += targetWindowBytesDelta;
        acceptReplyWindowFrames += targetWindowFramesDelta;

        if (slotIndex != NO_SLOT)
        {
            decodeBufferedData();
        }

        final int sourceWindowBytesDelta = targetWindowBytesDelta + connectReplyWindowBytesAdjustment;
        final int sourceWindowFramesDelta = targetWindowFramesDelta + connectReplyWindowFramesAdjustment;

        connectReplyWindowBytes += Math.max(sourceWindowBytesDelta, 0);
        connectReplyWindowBytesAdjustment = Math.min(sourceWindowBytesDelta, 0);

        connectReplyWindowFrames += Math.max(sourceWindowFramesDelta, 0);
        connectReplyWindowFramesAdjustment = Math.min(sourceWindowFramesDelta, 0);

        if (sourceWindowBytesDelta > 0 || sourceWindowFramesDelta > 0)
        {
            int windowUpdate = Math.max(sourceWindowBytesDelta, 0);
            factory.writer.doWindow(connectReplyThrottle, sourceId, windowUpdate, windowUpdate);
        }
    }

    private void handleReset(
        ResetFW reset)
    {
        releaseSlotIfNecessary();
        factory.writer.doReset(connectReplyThrottle, sourceId);
        connection.persistent = false;
        connectionPool.release(connection, false);
    }

    private void releaseSlotIfNecessary()
    {
        if (slotIndex != NO_SLOT)
        {
            factory.slab.release(slotIndex);
            slotIndex = NO_SLOT;
        }
    }

    @FunctionalInterface interface DecoderState
    {
        int decode(DirectBuffer buffer, int offset, int limit);
    }

}