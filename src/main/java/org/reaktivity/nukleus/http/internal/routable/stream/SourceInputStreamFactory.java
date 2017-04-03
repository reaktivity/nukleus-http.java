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
import static org.reaktivity.nukleus.http.internal.router.RouteKind.OUTPUT_ESTABLISHED;
import static org.reaktivity.nukleus.http.internal.util.BufferUtil.limitOfBytes;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

    private final FrameFW frameRO = new FrameFW();

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();

    private final Source source;
    private final LongFunction<List<Route>> supplyRoutes;
    private final LongSupplier supplyStreamId;
    private final Target rejectTarget;
    private final LongObjectBiConsumer<Correlation> correlateNew;
    private final int maximumHeadersSize;
    private final Slab slab;

    public SourceInputStreamFactory(
        Source source,
        LongFunction<List<Route>> supplyRoutes,
        LongSupplier supplyStreamId,
        Target rejectTarget,
        LongObjectBiConsumer<Correlation> correlateNew,
        int maximumHeadersSize,
        int memoryForDecode)
    {
        this.source = source;
        this.supplyRoutes = supplyRoutes;
        this.supplyStreamId = supplyStreamId;
        this.rejectTarget = rejectTarget;
        this.correlateNew = correlateNew;
        this.maximumHeadersSize = maximumHeadersSize;
        this.slab = new Slab(memoryForDecode, maximumHeadersSize);
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
        private int slotIndex;
        private int slotPosition;

        private long sourceId;

        private Target target;
        private long targetId;
        private long sourceRef;
        private long correlationId;
        private int window;
        private int contentRemaining;
        private int sourceUpdateDeferred;

        @Override
        public String toString()
        {
            return String.format("%s[source=%s, sourceId=%016x, window=%d, targetId=%016x]",
                    getClass().getSimpleName(), source.routableName(), sourceId, window, targetId);
        }

        private SourceInputStream()
        {
            this.streamState = this::streamBeforeBegin;
            this.throttleState = this::beforeReset;
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

        private void processInvalidRequest(
            int requestBytes,
            String payloadChars)
        {
            this.target = rejectTarget;

            final long newTargetId = supplyStreamId.getAsLong();

            // TODO: replace with connection pool (start)
            target.doBegin(newTargetId, 0L, correlationId);
            //target.addThrottle(newTargetId, this::handleThrottle);
            // TODO: replace with connection pool (end)

            // TODO: acquire slab for response if targetWindow requires partial write, defer the write till
            //       we have available (reject) targetWindow
            DirectBuffer payload = new UnsafeBuffer(payloadChars.getBytes(StandardCharsets.UTF_8));
            target.doData(newTargetId, payload, 0, payload.capacity());

            this.decoderState = this::decodeHttpBegin;
            this.streamState = this::streamAfterReplyOrReset;
            this.throttleState = this::beforeWindowOrReset;
            window += requestBytes;
            source.doWindow(sourceId, requestBytes);
        }

        private void processBegin(
            DirectBuffer buffer,
            int index,
            int length)
        {
            beginRO.wrap(buffer, index, index + length);

            this.sourceId = beginRO.streamId();
            this.sourceRef = beginRO.referenceId();
            this.correlationId = beginRO.correlationId();

            this.streamState = this::streamAfterBeginOrData;
            this.decoderState = this::decodeHttpBegin;

            this.window += maximumHeadersSize;

            // Make sure we don't advertise more window than available target window
            // once we have started the stream on the target
            this.sourceUpdateDeferred -= maximumHeadersSize;

            source.doWindow(sourceId, maximumHeadersSize);
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
                while (offset < limit)
                {
                    offset = decoderState.decode(buffer, offset, limit);
                }
            }
        }

        private void processEnd(
            DirectBuffer buffer,
            int index,
            int length)
        {
            endRO.wrap(buffer, index, index + length);
            final long streamId = endRO.streamId();

            decoderState = (b, o, l) -> o;

            source.removeStream(streamId);
            target.removeThrottle(targetId);
        }

        private int defragmentHttpBegin(
            final DirectBuffer payload,
            final int offset,
            final int limit)
        {
            final int endOfHeadersAt = limitOfBytes(payload, offset, limit, CRLFCRLF_BYTES);
            if (window < 2 && endOfHeadersAt == -1)
            {
                slab.release(slotIndex);
                processInvalidRequest(limit - offset, "HTTP/1.1 431 Request Header Fields Too Large\r\n\r\n");
                return limit;
            }
            else
            {
                int length = endOfHeadersAt == -1 ? limit - offset : endOfHeadersAt - offset;
                MutableDirectBuffer buffer = slab.buffer(slotIndex);
                buffer.putBytes(slotPosition, payload, offset, length);
                slotPosition += length;
                if (endOfHeadersAt == -1)
                {
                    return limit;
                }
                else
                {
                    decodeCompleteHttpBegin(buffer, 0, slotPosition);
                    slab.release(slotIndex);
                    return endOfHeadersAt;
                }
            }
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
                slotPosition = 0;
                int length = limit - offset;
                MutableDirectBuffer buffer = slab.buffer(slotIndex);
                assert length <= buffer.capacity();
                buffer.putBytes(0, payload, offset, length);
                slotPosition = length;
                decoderState = this::defragmentHttpBegin;
                return limit;
            }
            else
            {
                decodeCompleteHttpBegin(payload, offset, endOfHeadersAt - offset);
                return endOfHeadersAt;
            }
        }

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
                processInvalidRequest(length, "HTTP/1.1 400 Bad Request\r\n\r\n");
                return;
            }

            Pattern versionPattern = Pattern.compile("HTTP/1\\.(\\d)");
            Matcher versionMatcher = versionPattern.matcher(start[2]);
            if (!versionMatcher.matches())
            {
                processInvalidRequest(length, "HTTP/1.1 505 HTTP Version Not Supported\r\n\r\n");
            }
            else
            {
                final URI requestURI = URI.create(start[1]);

                final Map<String, String> headers = decodeHttpHeaders(start, lines, requestURI);
                // TODO: replace with lightweight approach (end)

                if (headers.get(":authority") == null || requestURI.getUserInfo() != null)
                {
                    processInvalidRequest(length, "HTTP/1.1 400 Bad Request\r\n\r\n");
                }
                else
                {
                    final Optional<Route> optional = resolveTarget(sourceRef, headers);
                    if (optional.isPresent())
                    {
                        final long newTargetId = supplyStreamId.getAsLong();
                        final long targetCorrelationId = newTargetId;
                        final Correlation correlation = new Correlation(correlationId, source.routableName(), OUTPUT_ESTABLISHED);

                        correlateNew.accept(targetCorrelationId, correlation);

                        final Route route = optional.get();
                        final Target newTarget = route.target();
                        final long targetRef = route.targetRef();

                        newTarget.doHttpBegin(newTargetId, targetRef, targetCorrelationId,
                                hs -> headers.forEach((k, v) -> hs.item(i -> i.name(k).value(v))));
                        newTarget.addThrottle(newTargetId, this::handleThrottle);

                        boolean hasUpgrade = headers.containsKey("upgrade");

                        // TODO: wait for 101 first
                        if (hasUpgrade)
                        {
                            this.decoderState = this::decodeHttpDataAfterUpgrade;
                        }
                        else
                        {
                            this.contentRemaining = parseInt(headers.getOrDefault("content-length", "0"));
                            this.decoderState = this::decodeHttpData;
                        }

                        sourceUpdateDeferred += length;
                        this.target = newTarget;
                        this.targetId = newTargetId;
                        this.throttleState = this::beforeWindowOrReset;

                        if (!hasUpgrade && contentRemaining == 0)
                        {
                            // no content
                            newTarget.doHttpEnd(newTargetId);
                        }
                    }
                    else
                    {
                        processInvalidRequest(length, "HTTP/1.1 404 Not Found\r\n\r\n");
                    }
                }
            }
        }

        private Map<String, String> decodeHttpHeaders(
            String[] start,
            String[] lines,
            URI requestURI)
        {
            String authority = requestURI.getAuthority();

            Map<String, String> headers = new LinkedHashMap<>();
            headers.put(":scheme", "http");
            headers.put(":method", start[0]);
            headers.put(":path", requestURI.getPath());

            if (authority != null)
            {
                headers.put(":authority", authority);
            }

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

                // rfc7230#section-5.5
                if ("host".equals(name))
                {
                    if (authority == null)
                    {
                        headers.put(":authority", value);
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
                target.doHttpEnd(targetId);

                this.throttleState = this::beforeWindowOrReset;
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
            return limit;
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
            throttleState.onMessage(msgTypeId, buffer, index, length);
        }

        private void beforeReset(
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

        private void beforeWindowOrReset(
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
            sourceUpdateDeferred += update;
            if (sourceUpdateDeferred > 0)
            {
                window += sourceUpdateDeferred;
                source.doWindow(sourceId, sourceUpdateDeferred + framing(sourceUpdateDeferred));
                sourceUpdateDeferred = 0;
            }
        }


        private void processReset(
            DirectBuffer buffer,
            int index,
            int length)
        {
            resetRO.wrap(buffer, index, index + length);

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
