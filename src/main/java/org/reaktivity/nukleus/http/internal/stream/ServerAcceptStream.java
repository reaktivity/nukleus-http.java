/**
 * Copyright 2016-2018 The Reaktivity Project
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
import static org.reaktivity.nukleus.http.internal.util.BufferUtil.limitOfBytes;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.http.internal.stream.ServerStreamFactory.DecoderState;
import org.reaktivity.nukleus.http.internal.stream.ServerStreamFactory.HttpStatus;
import org.reaktivity.nukleus.http.internal.stream.ServerStreamFactory.StandardMethods;
import org.reaktivity.nukleus.http.internal.types.OctetsFW;
import org.reaktivity.nukleus.http.internal.types.control.HttpRouteExFW;
import org.reaktivity.nukleus.http.internal.types.control.RouteFW;
import org.reaktivity.nukleus.http.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http.internal.types.stream.FrameFW;
import org.reaktivity.nukleus.http.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.http.internal.types.stream.AbortFW;

final class ServerAcceptStream implements MessageConsumer
{
    private final HttpStatus httpStatus = new HttpStatus();

    private final MutableDirectBuffer temporarySlot;
    private final int maximumHeadersSize;


    private ServerStreamFactory factory;
    private final MessageConsumer acceptThrottle;
    private final long acceptId;
    private final long acceptRef;
    private final String acceptName;
    private final long acceptCorrelationId;
    private final long authorization;

    private MessageConsumer streamState;
    private MessageConsumer throttleState;
    private DecoderState decoderState;
    private int slotIndex = NO_SLOT;
    private int slotOffset = 0;
    private int slotPosition;
    private boolean endDeferred;

    private MessageConsumer target;
    private long targetId;
    private String targetName;
    private int sourceBudget;
    private int contentRemaining;
    private boolean isChunkedTransfer;
    private int chunkSizeRemaining;
    private int targetBudget;
    private int targetPadding;
    private boolean hasUpgrade;
    private Correlation<ServerAcceptState> correlation;
    private boolean targetBeginIssued;
    private Runnable cleanupConnectReply;
    private long acceptReplyId;
    private MessageConsumer acceptReply;
    private long throttleTraceId;
    private long streamTraceId;

    @Override
    public String toString()
    {
        return String.format("%s[source=%s, sourceId=%016x, window=%d, targetId=%016x]",
                getClass().getSimpleName(), acceptName, acceptId, sourceBudget, targetId);
    }

    ServerAcceptStream(ServerStreamFactory factory, MessageConsumer acceptThrottle,
                       long acceptId, long traceId, long acceptRef, String acceptName, long acceptCorrelationId,
                       long authorization)
    {
        this.factory = factory;
        this.streamState = this::streamBeforeBegin;
        this.throttleState = this::throttleIgnoreWindow;
        this.acceptThrottle = acceptThrottle;
        this.acceptId = acceptId;
        this.streamTraceId = traceId;
        this.acceptRef = acceptRef;
        this.acceptCorrelationId = acceptCorrelationId;
        this.authorization = authorization;
        this.acceptName = acceptName;
        this.temporarySlot = new UnsafeBuffer(ByteBuffer.allocateDirect(factory.bufferPool.slotCapacity()));
        this.maximumHeadersSize = factory.bufferPool.slotCapacity();
    }

    @Override
    public void accept(int msgTypeId, DirectBuffer buffer, int index, int length)
    {
        streamState.accept(msgTypeId, buffer, index, length);
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
        case AbortFW.TYPE_ID:
            processAbort(buffer, index, length);
            break;
        default:
            processUnexpected(buffer, index, length);
            break;
        }
    }

    private void streamBeforeEnd(
        int msgTypeId,
        DirectBuffer buffer,
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
        DirectBuffer buffer,
        int index,
        int length)
    {
        if (msgTypeId == DataFW.TYPE_ID)
        {
            DataFW data = factory.dataRO.wrap(buffer, index, index + length);
            final long streamId = data.streamId();

            factory.writer.doWindow(acceptThrottle, streamId, 0, data.length(), 0);
        }
        else if (msgTypeId == EndFW.TYPE_ID)
        {
            factory.endRO.wrap(buffer, index, index + length);
            this.streamState = this::streamAfterEnd;
        }
    }

    private void processUnexpected(
        DirectBuffer buffer,
        int index,
        int length)
    {
        FrameFW frame = factory.frameRO.wrap(buffer, index, index + length);
        long streamId = frame.streamId();

        processUnexpected(streamId);
    }

    private void processUnexpected(
        long streamId)
    {
        factory.writer.doReset(acceptThrottle, streamId, 0);
        this.streamState = this::streamAfterReset;
    }

    private void processInvalidRequest(int status, String message)
    {
        this.decoderState = this::decodeSkipData;
        this.streamState = this::streamAfterReset;
        releaseSlotIfNecessary();
        if (targetBeginIssued)
        {
            // Drain data from source before resetting to allow its writes to complete
            throttleState = ServerAcceptStream.this::throttlePropagateWindow;
            doSourceWindow(maximumHeadersSize, 0, 0);

            // We can't write back an HTTP error response because we already forwarded the request to the target
            factory.writer.doReset(acceptThrottle, acceptId, 0);
            factory.writer.doAbort(target, targetId, 0);
            if (correlation != null)
            {
                correlation.state().pendingRequests--;
            }

            doEnd(0L);
        }
        else
        {
            writeErrorResponse(status, message);
        }
    }

    private void releaseSlotIfNecessary()
    {
        if (slotIndex != NO_SLOT)
        {
            factory.bufferPool.release(slotIndex);
            slotIndex = NO_SLOT;
        }
    }

    private void writeErrorResponse(int status, String message)
    {
        long serverAcceptReplyStreamId = correlation.state().replyStreamId;
        switchTarget(acceptName, serverAcceptReplyStreamId);

        StringBuffer payloadText = new StringBuffer()
                .append(String.format("HTTP/1.1 %d %s\r\n", status, message))
                .append("Connection: close\r\n")
                .append("\r\n");

        final DirectBuffer payload = new UnsafeBuffer(payloadText.toString().getBytes(StandardCharsets.UTF_8));

        ServerAcceptState acceptState = correlation.state();
        int writableBytes = Math.max(Math.min(
                acceptState.acceptReplyBudget - acceptState.acceptReplyPadding, payload.capacity()), 0);
        if (writableBytes > 0)
        {
            acceptState.acceptReplyBudget -= writableBytes + acceptState.acceptReplyPadding;
            factory.writer.doData(target, targetId, 0, acceptState.acceptReplyPadding,
                    payload, 0, writableBytes);
        }
        if (writableBytes < payload.capacity())
        {
            this.throttleState = new MessageConsumer()
            {
                int offset = writableBytes;

                @Override
                public void accept(int msgTypeId, DirectBuffer buffer, int index, int length)
                {
                    switch (msgTypeId)
                    {
                    case WindowFW.TYPE_ID:
                        WindowFW window = factory.windowRO.wrap(buffer, index, index + length);
                        acceptState.acceptReplyBudget += window.credit();
                        acceptState.acceptReplyPadding = window.padding();
                        throttleTraceId = window.trace();
                        int writableBytes = Math.max(
                            Math.min(acceptState.acceptReplyBudget - acceptState.acceptReplyPadding,
                                     payload.capacity() - offset), 0);
                        if (writableBytes > 0)
                        {
                            acceptState.acceptReplyBudget -= writableBytes + acceptState.acceptReplyPadding;
                            ServerAcceptStream.this.factory.writer.doData(target, targetId, 0,
                                    acceptState.acceptReplyPadding, payload, offset, writableBytes);
                            offset += writableBytes;
                        }
                        if (offset == payload.capacity())
                        {
                            // Drain data from source before resetting to allow its writes to complete
                            throttleState = ServerAcceptStream.this::throttlePropagateWindow;
                            doSourceWindow(ServerAcceptStream.this.maximumHeadersSize, 0, window.trace());
                            factory.writer.doEnd(target, targetId, 0); // connection: close
                        }
                        break;
                    case ResetFW.TYPE_ID:
                        final ResetFW reset = factory.resetRO.wrap(buffer, index, index + length);
                        processReset(reset);
                        break;
                    default:
                        // ignore
                        break;
                    }
                }
            };
        }
        else
        {
            // Drain data from source before resetting to allow its writes to complete
            throttleState = ServerAcceptStream.this::throttlePropagateWindow;
            doSourceWindow(maximumHeadersSize, 0, 0);
            factory.writer.doEnd(target, targetId, 0); // connection: close
        }
    }

    private void processBegin(
        DirectBuffer buffer,
        int index,
        int length)
    {
        this.streamState = this::streamAfterBeginOrData;
        this.decoderState = this::decodeBeforeHttpBegin;

        final long acceptReplyId = factory.supplyReplyId.applyAsLong(acceptId);
        final MessageConsumer acceptReply = factory.router.supplyTarget(acceptName);
        final ServerAcceptState state = new ServerAcceptState(acceptName, acceptReplyId, acceptReply, factory.writer,
                 this::loopBackThrottle, factory.router, this::setCleanupConnectReply);
        factory.writer.doBegin(acceptReply, acceptReplyId, streamTraceId, 0L, acceptCorrelationId);
        this.correlation = new Correlation<>(acceptCorrelationId, acceptName, acceptReplyId, state);
        doSourceWindow(maximumHeadersSize, 0, 0);
        this.acceptReply = acceptReply;
        this.acceptReplyId = acceptReplyId;
    }

    void setCleanupConnectReply(Runnable cleanupConnectReply)
    {
        this.cleanupConnectReply = cleanupConnectReply;
    }

    private void processData(
        DirectBuffer buffer,
        int index,
        int length)
    {
        DataFW data = factory.dataRO.wrap(buffer, index, index + length);
        streamTraceId = data.trace();

        sourceBudget -= data.length() + data.padding();

        if (sourceBudget < 0)
        {
            processUnexpected(buffer, index, length);
        }
        else
        {
            final OctetsFW payload = data.payload();
            final int limit = payload.limit();
            int offset = payload.offset();

            offset = decode(payload.buffer(), offset, limit);

            if (offset < limit)
            {
                assert slotIndex == NO_SLOT;
                slotOffset = slotPosition = 0;
                slotIndex = factory.bufferPool.acquire(acceptId);
                if (slotIndex == NO_SLOT)
                {
                    // Out of factory.slab memory
                    processInvalidRequest(503, "Service Unavailable");
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
        EndFW end = factory.endRO.wrap(buffer, index, index + length);
        streamTraceId = end.trace();

        if (hasUpgrade)
        {
            factory.writer.doEnd(target, targetId, streamTraceId);
            decoderState = (b, o, l) -> o;
            streamState = this::streamAfterEnd;
            releaseSlotIfNecessary();
        }
        else
        {
            final long streamId = end.streamId();
            assert streamId == acceptId;
            doEnd(streamTraceId);
        }
    }

    private void processAbort(
            DirectBuffer buffer,
            int index,
            int length)
    {
        AbortFW abort = factory.abortRO.wrap(buffer, index, index + length);
        streamTraceId = abort.trace();
        Correlation correlation = factory.correlations.remove(acceptCorrelationId);
        factory.writer.doAbort(acceptReply, acceptReplyId, 0);
        if (targetBeginIssued)
        {
            factory.writer.doAbort(target, targetId, streamTraceId);
        }
        if (correlation == null &&  cleanupConnectReply != null)
        {
            cleanupConnectReply.run();
        }
        releaseSlotIfNecessary();
    }

    private void doEnd(long traceId)
    {
        decoderState = (b, o, l) -> o;
        streamState = this::streamAfterEnd;

        releaseSlotIfNecessary();

        if (correlation != null)
        {
            correlation.state().doEnd(factory.writer, traceId);
        }
    }

    private void deferAndProcessDataFrame(
        DirectBuffer buffer,
        int index,
        int length)
    {
        DataFW data = factory.dataRO.wrap(buffer, index, index + length);
        sourceBudget -= data.length() + data.padding();

        if (sourceBudget < 0)
        {
            processUnexpected(buffer, index, length);
        }
        else
        {
            final OctetsFW payload = data.payload();
            deferAndProcessData(payload.buffer(), payload.offset(), payload.limit());
        }
    }

    private void deferAndProcessData(DirectBuffer buffer, int offset, int limit)
    {
        final int dataLength = limit - offset;
        if (slotPosition + dataLength > factory.bufferPool.slotCapacity())
        {
            alignSlotData();
        }
        MutableDirectBuffer slot = factory.bufferPool.buffer(slotIndex);
        slot.putBytes(slotPosition, buffer, offset, dataLength);
        slotPosition += dataLength;
        processDeferredData();
        if (sourceBudget == 0)
        {
            // Increase source window to ensure we can receive the largest possible amount of data we can factory.slab
            int cachedBytes = slotPosition - slotOffset;
            ensureSourceWindow(factory.bufferPool.slotCapacity() - cachedBytes, targetPadding);
            if (sourceBudget == 0)
            {
                throw new IllegalStateException("Decoder failed to detect headers or chunk too long");
            }
        }
    }

    private void processDeferredData()
    {
        MutableDirectBuffer slot = factory.bufferPool.buffer(slotIndex);
        int offset = decode(slot, slotOffset, slotPosition);
        slotOffset = offset;
        if (slotOffset == slotPosition)
        {
            releaseSlotIfNecessary();
            streamState = this::streamAfterBeginOrData;
            if (endDeferred)
            {
                doEnd(0L);
            }
        }
    }

    private void deferEnd(
        DirectBuffer buffer,
        int index,
        int length)
    {
        EndFW end = factory.endRO.wrap(buffer, index, index + length);
        final long streamId = end.streamId();
        assert streamId == acceptId;

        endDeferred = true;
    }

    private void alignSlotData()
    {
        int dataLength = slotPosition - slotOffset;
        MutableDirectBuffer slot = factory.bufferPool.buffer(slotIndex);
        temporarySlot.putBytes(0, slot, slotOffset, dataLength);
        slot.putBytes(0, temporarySlot, 0, dataLength);
        slotOffset = 0;
        slotPosition = dataLength;
    }

    private int decodeHttpBegin(
            final DirectBuffer payload,
            final int offset,
            final int limit)
    {
        int result = limit;
        final int endOfHeadersAt = limitOfBytes(payload, offset, limit, ServerStreamFactory.CRLFCRLF_BYTES);
        if (endOfHeadersAt == -1)
        {
            // Incomplete request, signal we can't consume the data
            result = offset;

            int length = limit - offset;
            int firstSpaceCheckLimit = Math.min(offset + 1 + ServerStreamFactory.MAXIMUM_METHOD_BYTES, limit);
            int firstSpace = limitOfBytes(payload, offset, firstSpaceCheckLimit, ServerStreamFactory.SPACE);
            if (firstSpace != -1)
            {
                String method = payload.getStringWithoutLengthUtf8(offset, length).split("\\s+")[0];
                if (StandardMethods.parse(method) == null)
                {
                    processInvalidRequest(501, "Not Implemented");
                }
            }
            else  if (firstSpace == -1 && length > ServerStreamFactory.MAXIMUM_METHOD_BYTES)
            {
                processInvalidRequest(400, "Bad Request");
            }
            if (length >= maximumHeadersSize)
            {
                int firstCRLF = limitOfBytes(payload, offset, limit, ServerStreamFactory.CRLF_BYTES);
                if (firstCRLF == -1 || firstCRLF > maximumHeadersSize)
                {
                    processInvalidRequest(414, "Request URI too long");
                }
                else
                {
                    processInvalidRequest(431, "Request Header Fields Too Large");
                }
            }
        }
        else
        {
            decodeCompleteHttpBegin(payload, offset, endOfHeadersAt - offset);
            result = endOfHeadersAt;
        }
        return result;
    };

    private int decodeBeforeHttpBegin(
            final DirectBuffer payload,
            final int offset,
            final int limit)
    {
        int length = limit - offset;
        int result = offset;
        if (payload.getByte(offset) == '\r')
        {
            if (length > 1 && payload.getByte(offset+1) == '\n')
            {
                // RFC 3270 3.5.  Message Parsing Robustness: skip empty line (CRLF) before request-line
                result = offset + 2;
            }
        }
        else
        {
            decoderState = this::decodeHttpBegin;
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

        if (start.length != 3)
        {
            processInvalidRequest(400, "Bad Request");
            return;
        }

        Pattern versionPattern = Pattern.compile("HTTP/1\\.(\\d)");
        Matcher versionMatcher = versionPattern.matcher(start[2]);
        if (!versionMatcher.matches())
        {
            Pattern validVersionPattern = Pattern.compile("HTTP/(\\d)\\.(\\d)");
            Matcher validVersionMatcher = validVersionPattern.matcher(start[2]);
            if (validVersionMatcher.matches())
            {
                processInvalidRequest(505, "HTTP Version Not Supported");
            }
            else
            {
                processInvalidRequest(400, "Bad Request");
            }
        }
        else if (null == StandardMethods.parse(start[0]))
        {
            processInvalidRequest(501, "Not Implemented");
        }
        else
        {
            final URI requestURI = URI.create(start[1]);

            httpStatus.reset();
            final Map<String, String> headers = decodeHttpHeaders(start, lines, requestURI, httpStatus);

            // TODO: replace with lightweight approach (end)

            if (httpStatus.status != 200)
            {
                processInvalidRequest(httpStatus.status, httpStatus.message);
            }
            else if (headers.get(":authority") == null || requestURI.getUserInfo() != null)
            {
                processInvalidRequest(400, "Bad Request");
            }
            else
            {
                final RouteFW route = resolveTarget(acceptRef, authorization, headers);
                if (route != null)
                {

                    final String newTarget = route.target().asString();
                    final long targetRef = route.targetRef();
                    final long newTargetId = factory.supplyStreamId.getAsLong();

                    long newTargetCorrelationId = factory.supplyCorrelationId.getAsLong();
                    factory.correlations.put(newTargetCorrelationId, correlation);
                    correlation.state().pendingRequests++;

                    targetBudget = 0;
                    switchTarget(newTarget, newTargetId);
                    factory.writer.doHttpBegin(target, newTargetId, streamTraceId, targetRef, newTargetCorrelationId,
                            hs -> headers.forEach((k, v) -> hs.item(i -> i.representation((byte)0).name(k).value(v))));
                    targetBeginIssued = true;

                    hasUpgrade = headers.containsKey("upgrade");
                    String connectionOptions = headers.get("connection");
                    if (connectionOptions != null)
                    {
                        Arrays.asList(connectionOptions.toLowerCase().split(",")).stream().forEach((element) ->
                        {
                            if (element.equals("close"))
                            {
                                correlation.state().persistent = false;
                            }
                        });
                    }
                    if (hasUpgrade)
                    {
                        // TODO: wait for 101 first
                        decoderState = this::decodeHttpDataAfterUpgrade;
                        throttleState = this::throttleForHttpDataAfterUpgrade;
                        correlation.state().persistent = false;
                        correlation.state().endRequested = true;
                    }
                    else if (contentRemaining > 0)
                    {
                        decoderState = this::decodeHttpData;
                        throttleState = this::throttleForHttpData;
                    }
                    else if (isChunkedTransfer)
                    {
                        decoderState = this::decodeHttpChunk;
                        throttleState = this::throttleForHttpData;
                    }
                    else
                    {
                        // no content
                        httpRequestComplete();
                    }
                }
                else
                {
                    processInvalidRequest(404, "Not Found");
                }
            }
        }
    }

    private Map<String, String> decodeHttpHeaders(
        String[] start,
        String[] lines,
        URI requestURI,
        HttpStatus httpStatus)
    {
        String authority = requestURI.getAuthority();

        final Map<String, String> headers = new LinkedHashMap<>();
        headers.put(":scheme", "http");
        headers.put(":method", start[0]);
        headers.put(":path", requestURI.getRawPath());

        if (authority != null)
        {
            headers.put(":authority", authority);
        }

        Pattern headerPattern = Pattern.compile("([^\\s:]+):\\s*(.*)");
        boolean contentLengthFound = false;
        contentRemaining = 0;
        isChunkedTransfer = false;
        for (int i = 1; i < lines.length; i++)
        {
            Matcher headerMatcher = headerPattern.matcher(lines[i]);
            if (!headerMatcher.matches())
            {
                httpStatus.status = 400;
                httpStatus.message = "Bad Request";
                if (lines[i].startsWith(" "))
                {
                    httpStatus.message = "Bad Request - obsolete line folding not supported";
                }
                break;
            }

            String name = headerMatcher.group(1).toLowerCase();
            String value = headerMatcher.group(2);

            // rfc7230#section-5.5
            if ("host".equals(name))
            {
                if (authority == null)
                {
                    headers.put(":authority", value);
                }
            }
            else if ("transfer-encoding".equals(name))
            {
                if (contentLengthFound)
                {
                    httpStatus.status = 400;
                    httpStatus.message = "Bad Request";
                }
                else if (!"chunked".equals(value))
                {
                    // TODO: support other transfer encodings
                    httpStatus.status = 501;
                    httpStatus.message = "Unsupported transfer-encoding " + value;
                    break;
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
                    httpStatus.status = 400;
                    httpStatus.message = "Bad Request";
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
        int writableBytes = Math.min(length, contentRemaining);
        writableBytes = Math.min(targetBudget - targetPadding, writableBytes);

        if (writableBytes > 0)
        {
            factory.writer.doHttpData(target, targetId, streamTraceId, targetPadding, payload, offset, writableBytes);
            targetBudget -= writableBytes + targetPadding;
            contentRemaining -= writableBytes;
        }

        if (contentRemaining == 0)
        {
            httpRequestComplete();
        }

        return offset + Math.max(writableBytes, 0);
    };

    private int decodeHttpChunk(
            final DirectBuffer payload,
            final int offset,
            final int limit)
    {
        int result = limit;

        final int chunkHeaderLimit = limitOfBytes(payload, offset, limit, ServerStreamFactory.CRLF_BYTES);
        if (chunkHeaderLimit == -1)
        {
            result = offset;
        }
        else
        {
            final int colonAt = limitOfBytes(payload, offset, chunkHeaderLimit, ServerStreamFactory.SEMICOLON_BYTES);
            final int chunkSizeLimit = colonAt == -1 ? chunkHeaderLimit - 2 : colonAt - 1;
            final int chunkSizeLength = chunkSizeLimit - offset;

            try
            {
                final String chunkSizeHex = payload.getStringWithoutLengthUtf8(offset, chunkSizeLength);
                chunkSizeRemaining = Integer.parseInt(chunkSizeHex, 16);
            }
            catch (NumberFormatException ex)
            {
                processInvalidRequest(400,  "Bad Request");
            }

            if (chunkSizeRemaining == 0)
            {
                httpRequestComplete();
            }
            else
            {
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
                processInvalidRequest(400,  "Bad Request");
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
        int writableBytes = Math.min(length, chunkSizeRemaining);
        writableBytes = Math.min(targetBudget - targetPadding, writableBytes);

        if (writableBytes > 0)
        {
            factory.writer.doHttpData(target, targetId, streamTraceId, targetPadding, payload, offset, writableBytes);
            targetBudget -= writableBytes + targetPadding;
            chunkSizeRemaining -= writableBytes;
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
        int writableBytes = Math.min(length, targetBudget - targetPadding);
        if (writableBytes > 0)
        {
            factory.writer.doHttpData(target, targetId, streamTraceId, targetPadding, payload, offset, writableBytes);
            targetBudget -= writableBytes + targetPadding;
        }
        return offset + Math.max(writableBytes, 0);
    };

    private int decodeSkipData(
            final DirectBuffer payload,
            final int offset,
            final int limit)
    {
        doSourceWindow(limit - offset, 0, 0);
        return limit;
    };

    @SuppressWarnings("unused")
    private int decodeHttpEnd(
            final DirectBuffer payload,
            final int offset,
            final int limit)
    {
        // TODO: consider chunks, trailers
        factory.writer.doHttpEnd(target, targetId, streamTraceId);
        return limit;
    };

    private void httpRequestComplete()
    {
        factory.writer.doHttpEnd(target, targetId, streamTraceId);
        // TODO: target.removeThrottle(targetId);
        decoderState = this::decodeBeforeHttpBegin;
        throttleState = this::throttleIgnoreWindow;

        if (correlation.state().persistent)
        {
            this.streamState = this::streamAfterBeginOrData;
            this.decoderState = this::decodeBeforeHttpBegin;
            ensureSourceWindow(maximumHeadersSize, 0);
        }
        else
        {
            this.streamState = this::streamBeforeEnd;
        }
    }

    private RouteFW resolveTarget(
        long sourceRef,
        long authorization,
        Map<String, String> headers)
    {
        final MessagePredicate filter = (t, b, o, l) ->
        {
            final RouteFW route = factory.routeRO.wrap(b, o, o + l);
            final OctetsFW extension = route.extension();
            boolean headersMatch = true;
            if (extension.sizeof() > 0)
            {
                final HttpRouteExFW routeEx = extension.get(factory.routeExRO::wrap);
                headersMatch = routeEx.headers().anyMatch(
                        h -> !Objects.equals(h.value(), headers.get(h.name())));
            }
            return route.sourceRef() == sourceRef && headersMatch;
        };

        return factory.router.resolve(authorization, filter, (msgTypeId, buffer, index, length) ->
            factory.routeRO.wrap(buffer, index, index + length));
    }

    private void handleThrottle(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        // Ignore frames from a previous target input stream that has now ended
        FrameFW frame = factory.frameRO.wrap(buffer, index, index + length);
        long streamId = frame.streamId();
        if (streamId == targetId)
        {
            throttleState.accept(msgTypeId, buffer, index, length);
        }
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
            final ResetFW reset = factory.resetRO.wrap(buffer, index, index + length);
            processReset(reset);
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
            WindowFW window = factory.windowRO.wrap(buffer, index, index + length);
            processWindowForHttpData(window);
            break;
        case ResetFW.TYPE_ID:
            final ResetFW reset = factory.resetRO.wrap(buffer, index, index + length);
            processReset(reset);
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
            WindowFW window = factory.windowRO.wrap(buffer, index, index + length);
            processWindowForHttpDataAfterUpgrade(window);
            break;
        case ResetFW.TYPE_ID:
            final ResetFW reset = factory.resetRO.wrap(buffer, index, index + length);
            processReset(reset);
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
            WindowFW window = factory.windowRO.wrap(buffer, index, index + length);
            propagateWindow(window);
            break;
        case ResetFW.TYPE_ID:
            final ResetFW reset = factory.resetRO.wrap(buffer, index, index + length);
            processReset(reset);
            break;
        default:
            // ignore
            break;
        }
    }

    private void loopBackThrottle(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case WindowFW.TYPE_ID:
            WindowFW window = factory.windowRO.wrap(buffer, index, index + length);
            correlation.state().acceptReplyBudget += window.credit();
            correlation.state().acceptReplyPadding = window.padding();
            break;
        case ResetFW.TYPE_ID:
            final ResetFW reset = factory.resetRO.wrap(buffer, index, index + length);
            processReset(reset);
            break;
        default:
            // ignore
            break;
        }
    }

    private void processWindowForHttpData(
        WindowFW window)
    {
        targetBudget += window.credit();
        targetPadding = window.padding();
        throttleTraceId = window.trace();
        if (slotIndex != NO_SLOT)
        {
            processDeferredData();
        }
        ensureSourceWindow(Math.min(targetBudget, factory.bufferPool.slotCapacity()), targetPadding);
    }

    private void processWindowForHttpDataAfterUpgrade(
        WindowFW window)
    {
        targetBudget += window.credit();
        targetPadding = window.padding();
        throttleTraceId = window.trace();
        if (slotIndex != NO_SLOT)
        {
            processDeferredData();
        }
        if (slotIndex == NO_SLOT)
        {
            ensureSourceWindow(targetBudget, targetPadding);
            if (this.sourceBudget == targetBudget)
            {
                // Windows are now aligned
                throttleState = this::throttlePropagateWindow;
            }
        }
    }

    private void propagateWindow(
        WindowFW window)
    {
        int credit = window.credit();
        targetBudget += credit;
        targetPadding = window.padding();
        doSourceWindow(credit, targetPadding, window.trace());
    }

    private void ensureSourceWindow(int requiredWindow, int padding)
    {
        if (requiredWindow > sourceBudget)
        {
            int credit = requiredWindow - sourceBudget;
            doSourceWindow(credit, padding, 0L);
        }
    }

    private void doSourceWindow(int credit, int padding, long traceId)
    {
        sourceBudget += credit;
        factory.writer.doWindow(acceptThrottle, acceptId, traceId, credit, padding);
    }

    private void processReset(
        ResetFW reset)
    {
        throttleTraceId = reset.trace();
        releaseSlotIfNecessary();
        factory.writer.doReset(acceptThrottle, acceptId, throttleTraceId);
    }

    private void switchTarget(String newTargetName, long newTargetId)
    {
        // TODO: do we need to worry about removing the throttle on target (old target)?
        MessageConsumer newTarget = factory.router.supplyTarget(newTargetName);
        target = newTarget;
        targetId = newTargetId;
        targetName = newTargetName;
        targetBeginIssued = false;
        factory.router.setThrottle(targetName, newTargetId, this::handleThrottle);
        throttleState = this::throttleIgnoreWindow;
    }
}