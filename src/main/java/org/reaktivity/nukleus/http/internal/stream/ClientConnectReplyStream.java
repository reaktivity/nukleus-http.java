/**
 * Copyright 2016-2020 The Reaktivity Project
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
import static org.reaktivity.nukleus.budget.BudgetDebitor.NO_DEBITOR_INDEX;
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
import org.reaktivity.nukleus.budget.BudgetDebitor;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http.internal.stream.ConnectionPool.CloseAction;
import org.reaktivity.nukleus.http.internal.stream.ConnectionPool.Connection;
import org.reaktivity.nukleus.http.internal.types.OctetsFW;
import org.reaktivity.nukleus.http.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http.internal.types.stream.FrameFW;
import org.reaktivity.nukleus.http.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http.internal.types.stream.WindowFW;

final class ClientConnectReplyStream
{
    private final ClientStreamFactory factory;
    private final MessageConsumer connectReplyThrottle;
    private final long connectAffinity;

    private MessageConsumer throttleState;
    private DecoderState decoderState;
    private int chunkSize;

    private enum ResponseState
    {
        BEFORE_HEADERS, HEADERS, DATA, FINAL;
    };
    private ResponseState responseState;

    private int slotIndex = BufferPool.NO_SLOT;
    private int slotOffset = 0;
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
    private long acceptReplyDebitorId;
    private BudgetDebitor acceptReplyDebitor;
    private long acceptReplyDebitorIndex = NO_DEBITOR_INDEX;

    @Override
    public String toString()
    {
        return String.format("%s[connectReplyId=%016x, sourceBudget=%d, targetBudget=%d targetId=%016x]",
                getClass().getSimpleName(), connectReplyId, connectReplyBudget, acceptReplyBudget, acceptReplyId);
    }

    ClientConnectReplyStream(
        ClientStreamFactory factory,
        MessageConsumer connectReplyThrottle,
        long connectRouteId,
        long connectReplyId,
        long connectAffinity)
    {
        this.factory = factory;
        this.connectReplyThrottle = connectReplyThrottle;
        this.connectRouteId = connectRouteId;
        this.connectReplyId = connectReplyId;
        this.connectAffinity = connectAffinity;
        this.throttleState = this::handleThrottleBeforeBegin;
        this.windowHandler = this::handleWindow;
    }

    public void handleStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case BeginFW.TYPE_ID:
            final BeginFW begin = this.factory.beginRO.wrap(buffer, index, index + length);
            handleBegin(begin);
            break;
        case DataFW.TYPE_ID:
            final DataFW data = this.factory.dataRO.wrap(buffer, index, index + length);
            handleData(data);
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

    private void handleUnexpected(
        DirectBuffer buffer,
        int index,
        int length)
    {
        FrameFW frame = this.factory.frameRO.wrap(buffer, index, index + length);
        long streamId = frame.streamId();

        handleUnexpected(streamId, frame.traceId());
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

        doCleanup(CloseAction.ABORT);
    }

    private void handleInvalidResponseAndReset()
    {
        this.decoderState = this::decodeSkipData;

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

        connection.persistent = false;
        doCleanup(action);
    }

    private void handleBegin(
        BeginFW begin)
    {
        this.connectReplyId = begin.streamId();
        acceptReplyTraceId = begin.traceId();

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

    private void handleData(
        DataFW data)
    {
        acceptReplyTraceId = data.traceId();
        connectReplyBudget -= data.reserved();

        if (connectReplyBudget < 0)
        {
            handleUnexpected(data.streamId(), acceptReplyTraceId);
        }
        else
        {
            final OctetsFW payload = data.payload();
            DirectBuffer buffer = payload.buffer();
            int offset = payload.offset();
            int limit = payload.limit();

            if (slotIndex != NO_SLOT)
            {
                final MutableDirectBuffer slotBuffer = factory.bufferPool.buffer(slotIndex);
                slotBuffer.putBytes(slotOffset, buffer, offset, limit - offset);
                slotOffset += limit - offset;
                buffer = slotBuffer;
                offset = 0;
                limit = slotOffset;
            }
            decode(buffer, offset, limit);
        }
    }

    private void handleEnd(
        EndFW end)
    {
        final long streamId = end.streamId();
        assert streamId == connectReplyId;

        if (slotIndex != NO_SLOT && (responseState == ResponseState.BEFORE_HEADERS || responseState == ResponseState.DATA))
        {
            endDeferred = true;
        }
        else
        {
            if (responseState == ResponseState.BEFORE_HEADERS && acceptReply == null &&
                factory.correlations.get(connection.connectReplyId) == null)
            {
                responseState = ResponseState.FINAL;
            }
            else if (responseState == ResponseState.DATA && !connection.persistent)
            {
                factory.writer.doEnd(acceptReply, acceptRouteId, acceptReplyId, end.traceId());
                responseState = ResponseState.FINAL;
            }

            switch (responseState)
            {
            case BEFORE_HEADERS:
            case HEADERS:
            case DATA:
                // Incomplete response
                handleInvalidResponse(CloseAction.END, end.traceId());
                break;
            case FINAL:
                connection.persistent = false;
                doCleanup(CloseAction.END);
            }
        }
    }

    private void handleAbort(
        AbortFW abort)
    {
        final long streamId = abort.streamId();
        assert streamId == connectReplyId;

        if (responseState == ResponseState.BEFORE_HEADERS && acceptReply == null &&
                factory.correlations.get(connection.connectReplyId) == null)
        {
            responseState = ResponseState.FINAL;
        }
        else if (responseState == ResponseState.DATA && !connection.persistent)
        {
            factory.writer.doAbort(acceptReply, acceptRouteId, acceptReplyId, abort.traceId());
            responseState = ResponseState.FINAL;
        }

        switch (responseState)
        {
        case BEFORE_HEADERS:
        case HEADERS:
        case DATA:
            // Incomplete response
            handleInvalidResponse(CloseAction.ABORT, abort.traceId());
            break;
        case FINAL:
            connection.persistent = false;
            doCleanup(CloseAction.ABORT);
        }
    }

    private void decode(
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        DecoderState previous = null;
        while (offset <= limit && previous != decoderState)
        {
            previous = decoderState;
            offset = decoderState.decode(buffer, offset, limit);
        }

        if (offset < limit)
        {
            if (slotIndex == NO_SLOT)
            {
                slotIndex = factory.bufferPool.acquire(connectReplyId);
            }

            if (slotIndex == NO_SLOT)
            {
                factory.writer.doReset(connectReplyThrottle, connectRouteId, connectReplyId, factory.supplyTrace.getAsLong());
                connection.persistent = false;
                doCleanup(CloseAction.ABORT);
            }
            else
            {
                final MutableDirectBuffer slotBuffer = factory.bufferPool.buffer(slotIndex);
                slotBuffer.putBytes(0, buffer, offset, limit - offset);
                slotOffset = limit - offset;
            }
        }
        else if (slotIndex != NO_SLOT)
        {
            releaseSlotIfNecessary();
        }
        flushCredit();
    }

    private void flushCredit()
    {
        final int connectReplyCredit = factory.bufferPool.slotCapacity() - connectReplyBudget - slotOffset;
        final long traceId = factory.supplyTrace.getAsLong();
        if (connectReplyCredit > 0)
        {
            connectReplyBudget += connectReplyCredit;
            factory.writer.doWindow(connectReplyThrottle, connectRouteId, connectReplyId,
                traceId, connectReplyCredit, 0);
        }
    }

    private void doCleanup(CloseAction action)
    {
        decoderState = (b, o, l) -> o;
        responseState = ResponseState.FINAL;
        releaseSlotIfNecessary();
        cleanupResponseIfNecessary();
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
            factory.writer.doHttpBegin(acceptReply, acceptRouteId, acceptReplyId, acceptReplyTraceId, connectAffinity,
                hs -> headers.forEach((k, v) -> hs.item(i -> i.name(k).value(v))));

            // count all responses
            factory.countResponses.getAsLong();

            boolean upgraded = "101".equals(headers.get(":status"));
            String connectionOptions = headers.get("connection");
            if (connectionOptions != null)
            {
                Arrays.stream(connectionOptions.split("\\s*,\\s*")).forEach(element ->
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
                decoderState = this::decodeHttpResponseComplete;
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

        int progress = offset;

        if (writableBytes > 0)
        {
            final int maximum = writableBytes + acceptReplyPadding;
            final int minimum = Math.min(maximum, 1024);

            int claimed = maximum;
            if (acceptReplyDebitorIndex != NO_DEBITOR_INDEX)
            {
                claimed = acceptReplyDebitor.claim(acceptReplyDebitorIndex, acceptReplyId, minimum, maximum);
            }

            final int reserved = claimed;
            final int writableMax = reserved - acceptReplyPadding;
            if (writableMax > 0)
            {
                factory.writer.doHttpData(acceptReply, acceptRouteId, acceptReplyId, acceptReplyTraceId, acceptReplyPadding,
                        payload, offset, writableMax);
                acceptReplyBudget -= writableMax + acceptReplyPadding;
                contentRemaining -= writableMax;
                progress += writableMax;
            }
        }

        if (contentRemaining == 0)
        {
            decoderState = this::decodeHttpResponseComplete;
        }

        return progress;
    }

    private int decodeHttpChunk(
        final DirectBuffer payload,
        final int offset,
        final int limit)
    {
        int result = offset;

        final int chunkHeaderLimit = limitOfBytes(payload, offset, limit, CRLF_BYTES);
        if (chunkHeaderLimit != -1)
        {
            final int semicolonAt = limitOfBytes(payload, offset, chunkHeaderLimit, SEMICOLON_BYTES);
            final int chunkSizeLimit = semicolonAt == -1 ? chunkHeaderLimit - 2 : semicolonAt - 1;
            final int chunkSizeLength = chunkSizeLimit - offset;

            try
            {
                final String chunkSizeHex = payload.getStringWithoutLengthUtf8(offset, chunkSizeLength);
                chunkSize = Integer.parseInt(chunkSizeHex, 16);
                chunkSizeRemaining = chunkSize;

                contentRemaining += chunkSizeRemaining;
                decoderState = this::decodeHttpChunkData;
                result = chunkHeaderLimit;
            }
            catch (NumberFormatException ex)
            {
                handleInvalidResponseAndReset();
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
            if (payload.getByte(offset) != '\r' ||
                payload.getByte(offset + 1) != '\n')
            {
                handleInvalidResponseAndReset();
            }
            else
            {
                decoderState = this::decodeHttpChunk;
                result = offset + 2;
            }
        }

        if (chunkSize == 0)
        {
            decoderState = this::decodeHttpResponseComplete;
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

        int progress = offset;

        if (writableBytes > 0)
        {
            final int maximum = writableBytes + acceptReplyPadding;
            final int minimum = Math.min(maximum, 1024);

            int claimed = maximum;
            if (acceptReplyDebitorIndex != NO_DEBITOR_INDEX)
            {
                claimed = acceptReplyDebitor.claim(acceptReplyDebitorIndex, acceptReplyId, minimum, maximum);
            }

            final int required = claimed;
            final int writableMax = required - acceptReplyPadding;
            if (writableMax > 0)
            {
                factory.writer.doHttpData(acceptReply, acceptRouteId, acceptReplyId, acceptReplyTraceId, acceptReplyPadding,
                                          payload, offset, writableMax);
                acceptReplyBudget -= writableMax + acceptReplyPadding;
                chunkSizeRemaining -= writableMax;
                contentRemaining -= writableMax;

                progress += writableMax;
            }
        }

        if (chunkSizeRemaining == 0)
        {
            decoderState = this::decodeHttpChunkEnd;
        }

        return progress;
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

    private int decodeHttpResponseComplete(
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        factory.writer.doHttpEnd(acceptReply, acceptRouteId, acceptReplyId, factory.supplyTrace.getAsLong());
        acceptReply = null;

        cleanupResponseIfNecessary();

        if (connection.persistent)
        {
            httpResponseBegin();
        }
        else
        {
            this.responseState = ResponseState.FINAL;
        }

        connectionPool.release(connection, CloseAction.END);
        return offset;
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
        final long connectReplyTraceId = window.traceId();

        acceptReplyDebitorId = window.budgetId();
        acceptReplyBudget += window.credit();
        acceptReplyPadding = window.padding();

        if (acceptReplyDebitorId != 0L && acceptReplyDebitorIndex == NO_DEBITOR_INDEX)
        {
            acceptReplyDebitor = factory.supplyDebitor.apply(acceptReplyDebitorId);
            acceptReplyDebitorIndex = acceptReplyDebitor.acquire(acceptReplyDebitorId, acceptReplyId, this::doFlush);
        }

        doFlush(connectReplyTraceId);
    }

    private void doFlush(
        final long traceId)
    {
        if (slotIndex != NO_SLOT)
        {
            MutableDirectBuffer slot = factory.bufferPool.buffer(slotIndex);
            decode(slot, 0, slotOffset);
            if (slotIndex == NO_SLOT && endDeferred)
            {
                connection.persistent = false;
                if (contentRemaining > 0)
                {
                    factory.writer.doAbort(acceptReply, acceptRouteId, acceptReplyId, factory.supplyTrace.getAsLong());
                }
                doCleanup(CloseAction.END);
            }
        }
        else
        {
            flushCredit();
        }
    }

    private void handleReset(
        ResetFW reset)
    {
        releaseSlotIfNecessary();
        cleanupResponseIfNecessary();
        factory.writer.doReset(connectReplyThrottle, connectRouteId, connectReplyId, reset.traceId());
        connection.persistent = false;
        connectionPool.release(connection, CloseAction.ABORT);
    }

    private void releaseSlotIfNecessary()
    {
        if (slotIndex != NO_SLOT)
        {
            factory.bufferPool.release(slotIndex);
            slotIndex = NO_SLOT;
            slotOffset = 0;
        }
    }

    private void cleanupResponseIfNecessary()
    {
        if (acceptReplyDebitorIndex != NO_DEBITOR_INDEX)
        {
            acceptReplyDebitor.release(acceptReplyDebitorIndex, acceptReplyId);
            acceptReplyDebitorIndex = NO_DEBITOR_INDEX;
            acceptReplyDebitor = null;
        }
    }

    @FunctionalInterface interface DecoderState
    {
        int decode(DirectBuffer buffer, int offset, int limit);
    }
}

