/**
 * Copyright 2016-2019 The Reaktivity Project
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
import org.reaktivity.nukleus.http.internal.stream.ConnectionPool.CloseAction;
import org.reaktivity.nukleus.http.internal.stream.ConnectionPool.Connection;
import org.reaktivity.nukleus.http.internal.types.OctetsFW;
import org.reaktivity.nukleus.http.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.http.internal.types.stream.FrameFW;

final class ClientConnectReplyStream implements MessageConsumer
{
    private final ClientStreamFactory factory;
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

    private long connectRouteId;
    private long connectReplyId;

    private MessageConsumer acceptReply;
    private long acceptRouteId;
    private long acceptReplyId;
    private long acceptReplyTraceId;

    private int contentRemaining;
    private boolean isChunkedTransfer;
    private int chunkSizeRemaining;
    private ConnectionPool connectionPool;
    private Connection connection;

    private int connectReplyBudget;
    private int acceptReplyBudget;
    private Consumer<WindowFW> windowHandler;

    private int acceptReplyPadding;

    @Override
    public String toString()
    {
        return String.format("%s[connectReplyId=%016x, sourceBudget=%d, targetId=%016x]",
            getClass().getSimpleName(), connectReplyId, connectReplyBudget, acceptReplyId);
    }

    ClientConnectReplyStream(
        ClientStreamFactory factory,
        MessageConsumer connectReplyThrottle,
        long connectRouteId,
        long connectReplyId)
    {
        this.factory = factory;
        this.connectReplyThrottle = connectReplyThrottle;
        this.connectRouteId = connectRouteId;
        this.connectReplyId = connectReplyId;
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
            case AbortFW.TYPE_ID:
                final AbortFW abort = this.factory.abortRO.wrap(buffer, index, index + length);
                handleAbort(abort);
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
            case AbortFW.TYPE_ID:
                final AbortFW abort = this.factory.abortRO.wrap(buffer, index, index + length);
                handleAbort(abort);
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
            case AbortFW.TYPE_ID:
                final AbortFW abort = this.factory.abortRO.wrap(buffer, index, index + length);
                handleAbort(abort);
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
                factory.writer.doWindow(connectReplyThrottle, connectRouteId, data.streamId(), 0, data.length(), 0);
                break;
            case EndFW.TYPE_ID:
                this.factory.endRO.wrap(buffer, index, index + length);
                this.streamState = this::handleStreamAfterEnd;
                break;
            case AbortFW.TYPE_ID:
                this.factory.abortRO.wrap(buffer, index, index + length);
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
        FrameFW frame = this.factory.frameRO.wrap(buffer, index, index + length);
        long streamId = frame.streamId();

        handleUnexpected(streamId, frame.trace());
    }

    private void handleUnexpected(
        long streamId,
        long traceId)
    {
        factory.writer.doReset(connectReplyThrottle, connectRouteId, streamId, factory.supplyTrace.getAsLong());
        if (acceptReply != null)
        {
            factory.writer.doAbort(acceptReply, acceptRouteId, acceptReplyId, traceId);
        }

        this.streamState = this::handleStreamAfterReset;
        doCleanup(CloseAction.ABORT);
    }

    private void handleInvalidResponseAndReset()
    {
        this.decoderState = this::decodeSkipData;
        this.streamState = this::handleStreamAfterReset;

        // Drain data from source before resetting to allow its writes to complete
        int window = factory.maximumHeadersSize;
        factory.writer.doWindow(connectReplyThrottle, connectRouteId, connectReplyId, factory.supplyTrace.getAsLong(),
            window, 0);
        factory.writer.doReset(connectReplyThrottle, connectRouteId, connectReplyId, factory.supplyTrace.getAsLong());

        connection.persistent = false;
        doCleanup(CloseAction.ABORT);
    }

    private void handleInvalidResponse(CloseAction action, long traceId)
    {
        if (acceptReply != null)
        {
            factory.writer.doAbort(acceptReply, acceptRouteId, acceptReplyId, traceId);

            // count abandoned responses
            factory.countResponsesAbandoned.getAsLong();
        }

        this.decoderState = this::decodeSkipData;
        this.streamState = this::handleStreamAfterReset;

        connection.persistent = false;
        doCleanup(action);
    }

    private void handleBegin(
        BeginFW begin)
    {
        this.connectReplyId = begin.streamId();
        acceptReplyTraceId = begin.trace();

        @SuppressWarnings("unchecked")
        final Correlation<ClientConnectReplyState> correlation =
            (Correlation<ClientConnectReplyState>) factory.correlations.get(connectReplyId);
        if (correlation != null)
        {
            connection = correlation.state().connection;
            connectionPool = correlation.state().connectionPool;
            httpResponseBegin();
        }
        else
        {
            handleUnexpected(connectReplyId, acceptReplyTraceId);
        }
    }

    private void handleDataWhenNotBuffering(
        DataFW data)
    {
        acceptReplyTraceId = data.trace();
        connectReplyBudget -= data.length() + data.padding();

        if (connectReplyBudget < 0)
        {
            handleUnexpected(data.streamId(), acceptReplyTraceId);
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
        assert streamId == connectReplyId;

        if (responseState == ResponseState.BEFORE_HEADERS && acceptReply == null
            && factory.correlations.get(connection.connectReplyId) == null)
        {
            responseState = ResponseState.FINAL;
        }
        else if (responseState == ResponseState.DATA && !connection.persistent)
        {
            factory.writer.doEnd(acceptReply, acceptRouteId, acceptReplyId, end.trace());
            responseState = ResponseState.FINAL;
        }

        switch (responseState)
        {
            case BEFORE_HEADERS:
            case HEADERS:
            case DATA:
                // Incomplete response
                handleInvalidResponse(CloseAction.END, end.trace());
                break;
            case FINAL:
                connection.persistent = false;
                doCleanup(CloseAction.END);
        }
    }

    private void handleAbort(
        AbortFW abort)
    {
        final long streamId = abort.streamId();
        assert streamId == connectReplyId;

        if (responseState == ResponseState.BEFORE_HEADERS && acceptReply == null
            && factory.correlations.get(connection.connectReplyId) == null)
        {
            responseState = ResponseState.FINAL;
        }
        else if (responseState == ResponseState.DATA && !connection.persistent)
        {
            factory.writer.doAbort(acceptReply, acceptRouteId, acceptReplyId, abort.trace());
            responseState = ResponseState.FINAL;
        }

        switch (responseState)
        {
            case BEFORE_HEADERS:
            case HEADERS:
            case DATA:
                // Incomplete response
                handleInvalidResponse(CloseAction.ABORT, abort.trace());
                break;
            case FINAL:
                connection.persistent = false;
                doCleanup(CloseAction.ABORT);
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
        slotIndex = factory.bufferPool.acquire(connectReplyId);
        if (slotIndex == NO_SLOT)
        {
            // Out of slab memory
            factory.writer.doReset(connectReplyThrottle, connectRouteId, connectReplyId, factory.supplyTrace.getAsLong());
            connection.persistent = false;
            doCleanup(CloseAction.ABORT);
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
        acceptReplyTraceId = data.trace();
        connectReplyBudget -= data.length() + data.padding();

        if (connectReplyBudget < 0)
        {
            handleUnexpected(data.streamId(), acceptReplyTraceId);
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

        if (slotPosition + payloadSize > factory.bufferPool.slotCapacity())
        {
            alignSlotData();
        }

        MutableDirectBuffer slot = factory.bufferPool.buffer(slotIndex);
        slot.putBytes(slotPosition, payload.buffer(), payload.offset(), payloadSize);
        slotPosition += payloadSize;
    }

    private void decodeBufferedData()
    {
        MutableDirectBuffer slot = factory.bufferPool.buffer(slotIndex);
        slotOffset = decode(slot, slotOffset, slotPosition);
        if (slotOffset == slotPosition)
        {
            releaseSlotIfNecessary();
            slotIndex = NO_SLOT;
            streamState = this::handleStreamWhenNotBuffering;
            if (endDeferred)
            {
                connection.persistent = false;
                if (contentRemaining > 0)
                {
                    factory.writer.doAbort(acceptReply, acceptRouteId, acceptReplyId, factory.supplyTrace.getAsLong());
                }
                doCleanup(CloseAction.END);
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
        assert streamId == connectReplyId;

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
        MutableDirectBuffer slot = factory.bufferPool.buffer(slotIndex);
        factory.temporarySlot.putBytes(0, slot, slotOffset, dataLength);
        slot.putBytes(0, factory.temporarySlot, 0, dataLength);
        slotOffset = 0;
        slotPosition = dataLength;
    }

    private void doCleanup(CloseAction action)
    {
        decoderState = (b, o, l) -> o;
        streamState = this::handleStreamAfterEnd;
        responseState = ResponseState.FINAL;
        releaseSlotIfNecessary();
        if (connection != null)
        {
            connectionPool.release(connection, action);
        }
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
                handleInvalidResponseAndReset();
                result = offset + factory.maximumHeadersSize;
            }
        }
        else
        {
            final int sizeofHeaders = endOfHeadersAt - offset;
            decodeCompleteHttpBegin(payload, offset, sizeofHeaders);
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
            handleInvalidResponseAndReset();
        }
        else
        {
            final Map<String, String> headers = decodeHttpHeaders(start, lines);
            // TODO: replace with lightweight approach (end)

            resolveTarget();

            factory.router.setThrottle(acceptReplyId, this::handleThrottle);
            factory.writer.doHttpBegin(acceptReply, acceptRouteId, acceptReplyId, acceptReplyTraceId,
                hs -> headers.forEach((k, v) -> hs.item(i -> i.name(k).value(v))));

            // count all responses
            factory.countResponses.getAsLong();

            boolean upgraded = "101".equals(headers.get(":status"));
            String connectionOptions = headers.get("connection");
            if (connectionOptions != null)
            {
                Arrays.stream(connectionOptions.split("\\s*,\\s*")).forEach((element) ->
                {
                    if (element.equalsIgnoreCase("close"))
                    {
                        connection.persistent = false;
                    }
                });
            }

            if (upgraded)
            {
                connection.persistent = false;
                connection.upgraded = true;
                connectionPool.release(connection);
                this.decoderState = this::decodeHttpDataAfterUpgrade;
                throttleState = this::handleThrottleAfterBegin;
                windowHandler = this::handleWindow;
                this.responseState = ResponseState.DATA;
            }
            else if (contentRemaining > 0)
            {
                decoderState = this::decodeHttpData;
                throttleState = this::handleThrottleAfterBegin;
                windowHandler = this::handleWindow;
                this.responseState = ResponseState.DATA;
            }
            else if (isChunkedTransfer)
            {
                decoderState = this::decodeHttpChunk;
                throttleState = this::handleThrottleAfterBegin;
                windowHandler = this::handleWindow;
                this.responseState = ResponseState.DATA;
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
                    handleInvalidResponseAndReset();
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
                    handleInvalidResponseAndReset();
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
        final int writableBytes = Math.min(acceptReplyBudget - acceptReplyPadding, remainingBytes);

        if (writableBytes > 0)
        {
            factory.writer.doHttpData(acceptReply, acceptRouteId, acceptReplyId, acceptReplyTraceId, acceptReplyPadding, payload,
                offset, writableBytes);
            acceptReplyBudget -= writableBytes + acceptReplyPadding;
            contentRemaining -= writableBytes;
        }

        if (contentRemaining == 0)
        {
            httpResponseComplete();
        }

        return offset + Math.max(writableBytes, 0);
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
                handleInvalidResponseAndReset();
            }

            if (chunkSizeRemaining == 0)
            {
                httpResponseComplete();
            }
            else
            {
                contentRemaining += chunkSizeRemaining;

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
                handleInvalidResponseAndReset();
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
        final int writableBytes = Math.min(acceptReplyBudget - acceptReplyPadding, remainingBytes);

        if (writableBytes > 0)
        {
            factory.writer.doHttpData(acceptReply, acceptRouteId, acceptReplyId, acceptReplyTraceId, acceptReplyPadding,
                payload, offset, writableBytes);
            acceptReplyBudget -= writableBytes + acceptReplyPadding;
            chunkSizeRemaining -= writableBytes;
            contentRemaining -= writableBytes;
        }

        if (chunkSizeRemaining == 0)
        {
            decoderState = this::decodeHttpChunkEnd;
        }

        return offset + Math.max(writableBytes, 0);
    }

    private int decodeHttpDataAfterUpgrade(
        final DirectBuffer payload,
        final int offset,
        final int limit)
    {
        final int length = limit - offset;

        final int writableBytes = Math.min(acceptReplyBudget - acceptReplyPadding, length);

        if (writableBytes > 0)
        {
            factory.writer.doData(acceptReply, acceptRouteId, acceptReplyId, acceptReplyTraceId, acceptReplyPadding,
                payload, offset, writableBytes);
            acceptReplyBudget -= writableBytes + acceptReplyPadding;
        }

        return offset + Math.max(writableBytes, 0);
    };

    private int decodeSkipData(
        final DirectBuffer payload,
        final int offset,
        final int limit)
    {
        factory.writer.doWindow(connectReplyThrottle, connectRouteId, connectReplyId, factory.supplyTrace.getAsLong(),
            limit - offset, 0);
        return limit;
    };

    @SuppressWarnings("unused")
    private int decodeHttpEnd(
        DirectBuffer payload,
        int offset,
        int limit)
    {
        // TODO: consider chunks, trailers
        factory.writer.doHttpEnd(acceptReply, acceptRouteId, acceptReplyId, acceptReplyTraceId);
        connectionPool.release(connection, CloseAction.END);
        return limit;
    }

    private void httpResponseBegin()
    {
        this.streamState = this::handleStreamWhenNotBuffering;
        this.decoderState = this::decodeHttpBegin;
        this.responseState = ResponseState.BEFORE_HEADERS;
        this.acceptReplyPadding = 0;

        final int connectReplyCredit = factory.maximumHeadersSize - connectReplyBudget;

        if (connectReplyCredit > 0)
        {
            this.connectReplyBudget += connectReplyCredit;
            factory.writer.doWindow(connectReplyThrottle, connectRouteId, connectReplyId, factory.supplyTrace.getAsLong(),
                connectReplyCredit, 0);
        }

        // TODO: Support HTTP/1.1 Pipelined Responses (may be buffered already)
        this.contentRemaining = 0;
    }

    private void httpResponseComplete()
    {
        factory.writer.doHttpEnd(acceptReply, acceptRouteId, acceptReplyId, factory.supplyTrace.getAsLong());
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

        connectionPool.release(connection, CloseAction.END);
    }

    private void resolveTarget()
    {
        final Correlation<?> correlation = factory.correlations.remove(connection.connectReplyId);
        this.acceptRouteId = correlation.routeId();
        this.acceptReplyId = correlation.replyId();
        this.acceptReply = correlation.reply();
        this.acceptReplyBudget = 0;
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

    private void handleWindow(
        WindowFW window)
    {
        acceptReplyBudget += window.credit();
        acceptReplyPadding = window.padding();

        if (slotIndex != NO_SLOT)
        {
            decodeBufferedData();
        }

        int slotRemaining = slotPosition - slotOffset;
        final int connectReplyCredit = Math.min(acceptReplyBudget, factory.bufferPool.slotCapacity())
            - connectReplyBudget - slotRemaining;
        if (connectReplyCredit > 0)
        {
            connectReplyBudget += connectReplyCredit;
            int connectReplyPadding = acceptReplyPadding;
            final long connectReplyTraceId = window.trace();
            factory.writer.doWindow(connectReplyThrottle, connectRouteId, connectReplyId,
                connectReplyTraceId, connectReplyCredit, connectReplyPadding);
        }
    }

    private void handleReset(
        ResetFW reset)
    {
        releaseSlotIfNecessary();
        factory.writer.doReset(connectReplyThrottle, connectRouteId, connectReplyId, reset.trace());
        connection.persistent = false;
        connectionPool.release(connection, CloseAction.ABORT);
    }

    private void releaseSlotIfNecessary()
    {
        if (slotIndex != NO_SLOT)
        {
            factory.bufferPool.release(slotIndex);
            slotIndex = NO_SLOT;
        }
    }

    @FunctionalInterface interface DecoderState
    {
        int decode(DirectBuffer buffer, int offset, int limit);
    }

}