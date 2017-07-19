package org.reaktivity.nukleus.http.internal.stream;

import static java.lang.Integer.parseInt;
import static org.reaktivity.nukleus.http.internal.routable.stream.Slab.NO_SLOT;
import static org.reaktivity.nukleus.http.internal.router.RouteKind.OUTPUT_ESTABLISHED;
import static org.reaktivity.nukleus.http.internal.util.BufferUtil.limitOfBytes;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.http.internal.routable.Correlation;
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
import org.reaktivity.nukleus.route.RouteHandler;

final class ServerAcceptStream implements MessageConsumer
{

    private final FrameFW frameRO = new FrameFW();
    private final RouteFW routeRO = new RouteFW();
    private final HttpRouteExFW routeExRO = new HttpRouteExFW();
    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();
    private final HttpStatus httpStatus = new HttpStatus();

    private final MutableDirectBuffer temporarySlot;
    private final int maximumHeadersSize;

    private final MessageWriter writer;
    private final BufferPool slab;
    private final RouteHandler router;
    private final LongSupplier supplyStreamId;
    private final Long2ObjectHashMap<Correlation<?>> correlations;
    private final MessageConsumer acceptThrottle;
    private final long acceptId;
    private final long acceptRef;
    private final String acceptName;
    private final long acceptCorrelationId;

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
    private int window;
    private int contentRemaining;
    private boolean isChunkedTransfer;
    private int chunkSizeRemaining;
    private int availableTargetWindow;
    private boolean hasUpgrade;
    private Correlation<ServerAcceptState> correlation;
    private boolean targetBeginIssued;

    @Override
    public String toString()
    {
        return String.format("%s[source=%s, sourceId=%016x, window=%d, targetId=%016x]",
                getClass().getSimpleName(), acceptName, acceptId, window, targetId);
    }

    ServerAcceptStream(MessageWriter writer, BufferPool slab, RouteHandler router, LongSupplier supplyStreamId,
                       Long2ObjectHashMap<Correlation<?>> correlations, MessageConsumer acceptThrottle,
                       long acceptId, long acceptRef, String acceptName, long acceptCorrelationId)
    {
        this.writer = writer;
        this.slab = slab;
        this.router = router;
        this.supplyStreamId = supplyStreamId;
        this.correlations = correlations;
        this.streamState = this::streamBeforeBegin;
        this.throttleState = this::throttleIgnoreWindow;
        this.acceptThrottle = acceptThrottle;
        this.acceptId = acceptId;
        this.acceptRef = acceptRef;
        this.acceptCorrelationId = acceptCorrelationId;
        this.acceptName = acceptName;
        this.temporarySlot = new UnsafeBuffer(ByteBuffer.allocateDirect(slab.slotCapacity()));
        this.maximumHeadersSize = slab.slotCapacity();
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
            dataRO.wrap(buffer, index, index + length);
            final long streamId = dataRO.streamId();

            writer.doWindow(acceptThrottle, streamId, dataRO.length(), dataRO.length());
        }
        else if (msgTypeId == EndFW.TYPE_ID)
        {
            this.endRO.wrap(buffer, index, index + length);
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
        writer.doReset(acceptThrottle, streamId);

        this.streamState = this::streamAfterReset;
    }

    private void processInvalidRequest(int status, String message)
    {
        this.decoderState = this::decodeSkipData;
        this.streamState = this::streamAfterReset;
        if (slotIndex != NO_SLOT)
        {
            slab.release(slotIndex);
            slotIndex = NO_SLOT;
        }
        if (targetBeginIssued)
        {
            // Drain data from source before resetting to allow its writes to complete
            throttleState = ServerAcceptStream.this::throttlePropagateWindow;
            doSourceWindow(maximumHeadersSize);

            // We can't write back an HTTP error response because we already forwarded the request to the target
            writer.doReset(acceptThrottle, acceptId);
            writer.doHttpEnd(target, targetId);
            doEnd();
        }
        else
        {
            writeErrorResponse(status, message);
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

        int writableBytes = Math.min(correlation.state().window, payload.capacity());
        if (writableBytes > 0)
        {
            writer.doData(target, targetId, payload, 0, writableBytes);
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
                        windowRO.wrap(buffer, index, index + length);
                        int update = ServerAcceptStream.this.windowRO.update();
                        int writableBytes = Math.min(update, payload.capacity() - offset);
                        ServerAcceptStream.this.writer.doData(target, targetId, payload, offset, writableBytes);
                        offset += writableBytes;
                        if (offset == payload.capacity())
                        {
                            // Drain data from source before resetting to allow its writes to complete
                            throttleState = ServerAcceptStream.this::throttlePropagateWindow;
                            doSourceWindow(ServerAcceptStream.this.maximumHeadersSize);
                            ServerAcceptStream.this.writer.doReset(acceptThrottle, acceptId);
                        }
                        break;
                    case ResetFW.TYPE_ID:
                        processReset(buffer, index, length);
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
            doSourceWindow(maximumHeadersSize);
            writer.doReset(acceptThrottle, acceptId);
        }
    }

    private void processBegin(
        DirectBuffer buffer,
        int index,
        int length)
    {
        beginRO.wrap(buffer, index, index + length);

        this.streamState = this::streamAfterBeginOrData;
        this.decoderState = this::decodeBeforeHttpBegin;

        // Proactively issue BEGIN on server accept reply since we only support bidirectional transport
        long replyStreamId = supplyStreamId.getAsLong();
        final MessageConsumer acceptReply = router.supplyTarget(acceptName);
        ServerAcceptState state = new ServerAcceptState(acceptName, replyStreamId, acceptReply, writer,
                 this::loopBackThrottle, router);
        writer.doBegin(acceptReply, replyStreamId, 0L, acceptCorrelationId);
        this.correlation = new Correlation<>(acceptCorrelationId, acceptName,
                OUTPUT_ESTABLISHED, state);

        doSourceWindow(maximumHeadersSize);
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
                slotIndex = slab.acquire(acceptId);
                if (slotIndex == NO_SLOT)
                {
                    // Out of slab memory
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
        endRO.wrap(buffer, index, index + length);
        final long streamId = endRO.streamId();
        assert streamId == acceptId;
        doEnd();
    }

    private void doEnd()
    {
        decoderState = (b, o, l) -> o;
        streamState = this::streamAfterEnd;

        slab.release(slotIndex);

        if (correlation != null)
        {
            correlation.state().doEnd(writer);
        }
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
        assert streamId == acceptId;

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
                final RouteFW route = resolveTarget(acceptRef, headers);
                if (route != null)
                {

                    final String newTarget = route.target().asString();
                    final long targetRef = route.targetRef();
                    final long newTargetId = supplyStreamId.getAsLong();

                    final long targetCorrelationId = newTargetId;
                    correlation.state().pendingRequests++;
                    correlations.put(targetCorrelationId, correlation);

                    availableTargetWindow = 0;
                    switchTarget(newTarget, newTargetId);
                    writer.doHttpBegin(target, newTargetId, targetRef, newTargetId,
                            hs -> headers.forEach((k, v) -> hs.item(i -> i.name(k).value(v))));
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
        headers.put(":scheme", "http");            headers.put(":method", start[0]);
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
        writableBytes = Math.min(availableTargetWindow, writableBytes);

        if (writableBytes > 0)
        {
            writer.doHttpData(target, targetId, payload, offset, writableBytes);
            availableTargetWindow -= writableBytes;
            contentRemaining -= writableBytes;
        }
        int result = offset + writableBytes;

        if (contentRemaining == 0)
        {
            httpRequestComplete();
        }
        return result;
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
        int result = offset;
        final int length = limit - offset;

        // TODO: consider chunks
        int writableBytes = Math.min(length, chunkSizeRemaining);
        writableBytes = Math.min(availableTargetWindow, writableBytes);

        if (writableBytes > 0)
        {
            writer.doHttpData(target, targetId, payload, offset, writableBytes);
            availableTargetWindow -= writableBytes;
            chunkSizeRemaining -= writableBytes;
        }
        result = offset + writableBytes;

        if (chunkSizeRemaining == 0)
        {
            decoderState = this::decodeHttpChunkEnd;
        }
        return result;
    }

    private int decodeHttpDataAfterUpgrade(
            final DirectBuffer payload,
            final int offset,
            final int limit)
    {
        final int length = limit - offset;
        int writableBytes = Math.min(length, availableTargetWindow);
        if (writableBytes > 0)
        {
            writer.doHttpData(target, targetId, payload, offset, writableBytes);
            availableTargetWindow -= writableBytes;
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
            final DirectBuffer payload,
            final int offset,
            final int limit)
    {
        // TODO: consider chunks, trailers
        writer.doHttpEnd(target, targetId);
        return limit;
    };

    private void httpRequestComplete()
    {
        writer.doHttpEnd(target, targetId);
        // TODO: target.removeThrottle(targetId);
        decoderState = this::decodeBeforeHttpBegin;
        throttleState = this::throttleIgnoreWindow;

        if (correlation.state().persistent)
        {
            this.streamState = this::streamAfterBeginOrData;
            this.decoderState = this::decodeBeforeHttpBegin;
            ensureSourceWindow(maximumHeadersSize);
        }
        else
        {
            this.streamState = this::streamBeforeEnd;
        }
    }

    private RouteFW resolveTarget(
        long sourceRef,
        Map<String, String> headers)
    {
        final MessagePredicate filter = (t, b, o, l) ->
        {
            final RouteFW route = routeRO.wrap(b, o, l);
            final OctetsFW extension = routeRO.extension();
            boolean headersMatch = true;
            if (extension.sizeof() > 0)
            {
                final HttpRouteExFW routeEx = extension.get(routeExRO::wrap);
                headersMatch = routeEx.headers().anyMatch(
                        h -> !Objects.equals(h.value(), headers.get(h.name())));
            }
            return route.sourceRef() == sourceRef && headersMatch;
        };

        return router.resolve(filter, (msgTypeId, buffer, index, length) ->
            routeRO.wrap(buffer, index, index + length));
    }

    private void handleThrottle(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        // Ignore frames from a previous target input stream that has now ended
        frameRO.wrap(buffer, index, index + length);
        long streamId = frameRO.streamId();
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

    private void loopBackThrottle(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case WindowFW.TYPE_ID:
            windowRO.wrap(buffer, index, index + length);
            int update = windowRO.update();
            correlation.state().window += update;
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
        writer.doWindow(acceptThrottle, acceptId, update, update);
    }

    private void processReset(
        DirectBuffer buffer,
        int index,
        int length)
    {
        resetRO.wrap(buffer, index, index + length);
        slab.release(slotIndex);
        writer.doReset(acceptThrottle, acceptId);
    }

    private void switchTarget(String newTargetName, long newTargetId)
    {
        // TODO: do we need to worry about removing the throttle on target (old target)?
        MessageConsumer newTarget = router.supplyTarget(newTargetName);
        target = newTarget;
        targetId = newTargetId;
        targetName = newTargetName;
        targetBeginIssued = false;
        router.setThrottle(targetName, newTargetId, this::handleThrottle);
        throttleState = this::throttleIgnoreWindow;
    }
}