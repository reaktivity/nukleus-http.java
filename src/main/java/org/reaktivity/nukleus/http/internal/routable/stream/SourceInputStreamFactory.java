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
import static org.reaktivity.nukleus.http.internal.routable.Route.headersMatch;
import static org.reaktivity.nukleus.http.internal.routable.stream.Slab.NO_SLOT;
import static org.reaktivity.nukleus.http.internal.router.RouteKind.OUTPUT_ESTABLISHED;
import static org.reaktivity.nukleus.http.internal.util.BufferUtil.limitOfBytes;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.http.internal.routable.Correlation;
import org.reaktivity.nukleus.http.internal.routable.Route;
import org.reaktivity.nukleus.http.internal.routable.Source;
import org.reaktivity.nukleus.http.internal.routable.Target;
import org.reaktivity.nukleus.http.internal.types.OctetsFW;
import org.reaktivity.nukleus.http.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http.internal.types.stream.FrameFW;
import org.reaktivity.nukleus.http.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.http.internal.util.function.LongObjectBiConsumer;

public final class SourceInputStreamFactory
{
    private static final byte[] CRLFCRLF_BYTES = "\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] CRLF_BYTES = "\r\n".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] COLON_BYTES = ";".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] SPACE = " ".getBytes(StandardCharsets.US_ASCII);
    private static final int MAXIMUM_METHOD_BYTES = "OPTIONS".length();

    private final FrameFW frameRO = new FrameFW();

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();

    private final Source source;
    private final LongFunction<List<Route>> supplyRoutes;
    private final LongSupplier supplyStreamId;
    private final Function<String, Target> supplyTarget;
    private final LongObjectBiConsumer<Correlation<?>> correlateNew;
    private final int maximumHeadersSize;
    private final Slab slab;
    private final MutableDirectBuffer temporarySlot;

    private final HttpStatus httpStatus = new HttpStatus();

    public SourceInputStreamFactory(
        Source source,
        LongFunction<List<Route>> supplyRoutes,
        LongSupplier supplyStreamId,
        Function<String, Target> supplyTarget,
        LongObjectBiConsumer<Correlation<?>> correlateNew,
        Slab slab)
    {
        this.source = source;
        this.supplyRoutes = supplyRoutes;
        this.supplyStreamId = supplyStreamId;
        this.supplyTarget = supplyTarget;
        this.correlateNew = correlateNew;
        this.slab = slab;
        this.maximumHeadersSize = slab.slotCapacity();
        this.temporarySlot = new UnsafeBuffer(ByteBuffer.allocateDirect(slab.slotCapacity()));
    }

    public MessageHandler newStream()
    {
        return new SourceInputStream()::handleStream;
    }

    private final class SourceInputStream
    {
        private MessageHandler streamState;
        private MessageHandler throttleState;
        private DecoderState decoderState;
        private int slotIndex = NO_SLOT;
        private int slotOffset = 0;
        private int slotPosition;
        private boolean endDeferred;

        private long sourceId;
        private long sourceCorrelationId;

        private Target target;
        private long targetId;
        private long sourceRef;
        private int window;
        private int contentRemaining;
        boolean isChunkedTransfer;
        int chunkSizeRemaining;
        private int availableTargetWindow;
        private boolean hasUpgrade;
        private Correlation<ServerAcceptState> correlation;
        private boolean targetBeginIssued;

        @Override
        public String toString()
        {
            return String.format("%s[source=%s, sourceId=%016x, window=%d, targetId=%016x]",
                    getClass().getSimpleName(), source.routableName(), sourceId, window, targetId);
        }

        private SourceInputStream()
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

        private void processInvalidRequest(int status, String message)
        {
            this.decoderState = decodeSkipData;
            this.streamState = this::streamAfterReset;
            if (slotIndex != NO_SLOT)
            {
                slab.release(slotIndex);
                slotIndex = NO_SLOT;
            }
            if (targetBeginIssued)
            {
                // Drain data from source before resetting to allow its writes to complete
                throttleState = SourceInputStream.this::throttlePropagateWindow;
                doSourceWindow(maximumHeadersSize);

                // We can't write back an HTTP error response because we already forwarded the request to the target
                source.doReset(sourceId);
                target.doHttpEnd(targetId);
                doEnd();
            }
            else
            {
                writeErrorResponse(status, message);
            }
        }

        private void writeErrorResponse(int status, String message)
        {
            Target serverAcceptReply = supplyTarget.apply(source.name());
            long serverAcceptReplyStreamId = correlation.state().streamId;
            switchTarget(serverAcceptReply, serverAcceptReplyStreamId);

            StringBuffer payloadText = new StringBuffer()
                    .append(String.format("HTTP/1.1 %d %s\r\n", status, message))
                    .append("Connection: close\r\n")
                    .append("\r\n");

            final DirectBuffer payload = new UnsafeBuffer(payloadText.toString().getBytes(StandardCharsets.UTF_8));

            int writableBytes = Math.min(correlation.state().window, payload.capacity());
            if (writableBytes > 0)
            {
                target.doData(targetId, payload, 0, writableBytes);
            }
            if (writableBytes < payload.capacity())
            {
                this.throttleState = new MessageHandler()
                {
                    int offset = writableBytes;

                    @Override
                    public void onMessage(int msgTypeId, MutableDirectBuffer buffer, int index, int length)
                    {
                        switch (msgTypeId)
                        {
                        case WindowFW.TYPE_ID:
                            windowRO.wrap(buffer, index, index + length);
                            int update = windowRO.update();
                            int writableBytes = Math.min(update, payload.capacity() - offset);
                            target.doData(targetId, payload, offset, writableBytes);
                            offset += writableBytes;
                            if (offset == payload.capacity())
                            {
                                // Drain data from source before resetting to allow its writes to complete
                                throttleState = SourceInputStream.this::throttlePropagateWindow;
                                doSourceWindow(maximumHeadersSize);
                                source.doReset(sourceId);
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
                throttleState = SourceInputStream.this::throttlePropagateWindow;
                doSourceWindow(maximumHeadersSize);
                source.doReset(sourceId);
            }
        }

        private void processBegin(
            DirectBuffer buffer,
            int index,
            int length)
        {
            beginRO.wrap(buffer, index, index + length);

            this.sourceId = beginRO.streamId();
            this.sourceRef = beginRO.referenceId();
            this.sourceCorrelationId = beginRO.correlationId();

            this.streamState = this::streamAfterBeginOrData;
            this.decoderState = decodeBeforeHttpBegin;

            // Proactively issue BEGIN on server accept reply since we only support bidirectional transport
            long replyStreamId = supplyStreamId.getAsLong();
            Target sourceReply = supplyTarget.apply(source.name());
            ServerAcceptState state = new ServerAcceptState(replyStreamId, sourceReply, this::loopBackThrottle);
            sourceReply.doBegin(replyStreamId, 0L, sourceCorrelationId);
            this.correlation = new Correlation<>(sourceCorrelationId, source.routableName(),
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
                    slotIndex = slab.acquire(sourceId);
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

            if (correlation != null)
            {
                correlation.state().doEnd(supplyTarget);
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
                int firstSpaceCheckLimit = Math.min(offset + 1 + MAXIMUM_METHOD_BYTES, limit);
                int firstSpace = limitOfBytes(payload, offset, firstSpaceCheckLimit, SPACE);
                if (firstSpace != -1)
                {
                    String method = payload.getStringWithoutLengthUtf8(offset, length).split("\\s+")[0];
                    if (StandardMethods.parse(method) == null)
                    {
                        processInvalidRequest(501, "Not Implemented");
                    }
                }
                else  if (firstSpace == -1 && length > MAXIMUM_METHOD_BYTES)
                {
                    processInvalidRequest(400, "Bad Request");
                }
                if (length >= maximumHeadersSize)
                {
                    int firstCRLF = limitOfBytes(payload, offset, limit, CRLF_BYTES);
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

        private final DecoderState decodeBeforeHttpBegin = (payload, offset, limit) ->
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
                decoderState = decodeHttpBegin;
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
                    final Optional<Route> optional = resolveTarget(sourceRef, headers);
                    if (optional.isPresent())
                    {

                        final Route route = optional.get();
                        final Target newTarget = route.target();
                        final long targetRef = route.targetRef();
                        final long newTargetId = supplyStreamId.getAsLong();

                        final long targetCorrelationId = newTargetId;
                        correlation.state().pendingRequests++;
                        correlateNew.accept(targetCorrelationId, correlation);

                        availableTargetWindow = 0;
                        switchTarget(newTarget, newTargetId);
                        newTarget.doHttpBegin(newTargetId, targetRef, newTargetId,
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
                            decoderState = decodeHttpDataAfterUpgrade;
                            throttleState = this::throttleForHttpDataAfterUpgrade;
                            correlation.state().persistent = false;
                        }
                        else if (contentRemaining > 0)
                        {
                            decoderState = decodeHttpData;
                            throttleState = this::throttleForHttpData;
                        }
                        else if (isChunkedTransfer)
                        {
                            decoderState = decodeHttpChunk;
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
                httpRequestComplete();
            }
            return result;
        };

        private DecoderState decodeHttpChunk = (payload, offset, limit) ->
        {
            int result = limit;
            final int endOfHeaderAt = limitOfBytes(payload, offset, limit, CRLF_BYTES);
            if (endOfHeaderAt == -1)
            {
                result = offset;
            }
            else
            {
                int colonAt = limitOfBytes(payload, offset, limit, COLON_BYTES);
                int chunkSizeLimit = colonAt == -1 ? endOfHeaderAt - 2 : colonAt - 1;
                int chunkSizeLength = chunkSizeLimit - offset;
                try
                {
                    chunkSizeRemaining = Integer.parseInt(payload.getStringWithoutLengthUtf8(offset, chunkSizeLength), 16);
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
                    result = endOfHeaderAt;
                }
            }
            return result;
        };

        private final DecoderState decodeHttpChunkEnd = (payload, offset, limit) ->
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
                    decoderState = decodeHttpChunk;
                    result = offset + 2;
                }
            }
            return result;
        };

        private int decodeHttpChunkData(DirectBuffer payload, int offset, int limit)
        {
            int result = offset;
            final int length = limit - offset;

            // TODO: consider chunks
            int writableBytes = Math.min(length, chunkSizeRemaining);
            writableBytes = Math.min(availableTargetWindow, writableBytes);

            if (writableBytes > 0)
            {
                target.doHttpData(targetId, payload, offset, writableBytes);
                availableTargetWindow -= writableBytes;
                chunkSizeRemaining -= writableBytes;
            }
            result = offset + writableBytes;

            if (chunkSizeRemaining == 0)
            {
                decoderState = decodeHttpChunkEnd;
            }
            return result;
        }

        private DecoderState decodeHttpDataAfterUpgrade = (payload, offset, limit) ->
        {
            final int length = limit - offset;
            int writableBytes = Math.min(length, availableTargetWindow);
            if (writableBytes > 0)
            {
                target.doHttpData(targetId, payload, offset, writableBytes);
                availableTargetWindow -= writableBytes;
            }
            return offset + writableBytes;
        };

        private DecoderState decodeSkipData = (payload, offset, limit) ->
        {
            return limit;
        };

        @SuppressWarnings("unused")
        private DecoderState decodeHttpEnd = (payload, offset, limit) ->
        {
            // TODO: consider chunks, trailers
            target.doHttpEnd(targetId);
            return limit;
        };

        private void httpRequestComplete()
        {
            target.doHttpEnd(targetId);
            // TODO: target.removeThrottle(targetId);
            decoderState = decodeBeforeHttpBegin;
            throttleState = this::throttleIgnoreWindow;

            if (correlation.state().persistent)
            {
                this.streamState = this::streamAfterBeginOrData;
                this.decoderState = decodeBeforeHttpBegin;
                ensureSourceWindow(maximumHeadersSize);
            }
            else
            {
                this.streamState = this::streamBeforeEnd;
            }
        }

        private Optional<Route> resolveTarget(
            long sourceRef,
            Map<String, String> headers)
        {
            final List<Route> routes = supplyRoutes.apply(sourceRef);
            final Predicate<Route> predicate = headersMatch(headers);

            return routes.stream().filter(predicate).findFirst();
        }

        private void handleThrottle(
            int msgTypeId,
            MutableDirectBuffer buffer,
            int index,
            int length)
        {
            // Ignore frames from a previous target input stream that has now ended
            frameRO.wrap(buffer, index, index + length);
            long streamId = frameRO.streamId();
            if (streamId == targetId)
            {
                throttleState.onMessage(msgTypeId, buffer, index, length);
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
            source.doWindow(sourceId, update);
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

        private void switchTarget(Target newTarget, long newTargetId)
        {
            if (target != null)
            {
                target.removeThrottle(targetId);
            }
            target = newTarget;
            targetId = newTargetId;
            targetBeginIssued = false;
            newTarget.setThrottle(newTargetId, this::handleThrottle);
            throttleState = SourceInputStream.this::throttleIgnoreWindow;
        }
    }

    @FunctionalInterface
    private interface DecoderState
    {
        int decode(DirectBuffer buffer, int offset, int limit);
    }

    private static final class HttpStatus
    {
        int status;
        String message;

        void reset()
        {
            status = 200;
            message = "OK";
        }
    }

    private enum StandardMethods
    {
        GET,
        HEAD,
        POST,
        PUT,
        DELETE,
        CONNECT,
        OPTIONS,
        TRACE;

        static StandardMethods parse(String name)
        {
            StandardMethods result;
            try
            {
                result = valueOf(name);
            }
            catch (IllegalArgumentException e)
            {
                result = null;
            }
            return result;
        }
    }

}
