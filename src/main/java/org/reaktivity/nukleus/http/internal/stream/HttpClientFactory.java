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

import static java.lang.Character.toLowerCase;
import static java.lang.Character.toUpperCase;
import static java.lang.Integer.parseInt;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Objects.requireNonNull;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.http.internal.util.BufferUtil.limitOfBytes;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.ToIntFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.agrona.AsciiSequenceView;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.http.internal.HttpConfiguration;
import org.reaktivity.nukleus.http.internal.HttpNukleus;
import org.reaktivity.nukleus.http.internal.types.Array32FW;
import org.reaktivity.nukleus.http.internal.types.Flyweight;
import org.reaktivity.nukleus.http.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http.internal.types.OctetsFW;
import org.reaktivity.nukleus.http.internal.types.String16FW;
import org.reaktivity.nukleus.http.internal.types.String8FW;
import org.reaktivity.nukleus.http.internal.types.control.HttpRouteExFW;
import org.reaktivity.nukleus.http.internal.types.control.RouteFW;
import org.reaktivity.nukleus.http.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http.internal.types.stream.FlushFW;
import org.reaktivity.nukleus.http.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http.internal.types.stream.HttpEndExFW;
import org.reaktivity.nukleus.http.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

public final class HttpClientFactory implements StreamFactory
{
    private static final Pattern RESPONSE_LINE_PATTERN =
            Pattern.compile("(?<version>HTTP/\\d\\.\\d)\\s+(?<status>\\d+)\\s+(?<reason>[^\\r\\n]+)\r\n");
    private static final Pattern VERSION_PATTERN = Pattern.compile("HTTP/1\\.\\d");
    private static final Pattern HEADER_LINE_PATTERN = Pattern.compile("(?<name>[^\\s:]+):\\s*(?<value>[^\r\n]*)\r\n");
    private static final Pattern CONNECTION_CLOSE_PATTERN = Pattern.compile("(^|\\s*,\\s*)close(\\s*,\\s*|$)");
    private static final Map<String, String> EMPTY_HEADERS = Collections.emptyMap();

    private static final byte[] HOST_BYTES = "Host".getBytes(US_ASCII);
    private static final byte[] COLON_SPACE_BYTES = ": ".getBytes(US_ASCII);
    private static final byte[] CRLFCRLF_BYTES = "\r\n\r\n".getBytes(US_ASCII);
    private static final byte[] CRLF_BYTES = "\r\n".getBytes(US_ASCII);
    private static final byte[] SEMICOLON_BYTES = ";".getBytes(US_ASCII);

    private static final byte COLON_BYTE = ':';
    private static final byte HYPHEN_BYTE = '-';
    private static final byte SPACE_BYTE = ' ';
    private static final byte ZERO_BYTE = '0';

    private static final byte[] HTTP_1_1_BYTES = "HTTP/1.1".getBytes(US_ASCII);

    private static final DirectBuffer ZERO_CHUNK = new UnsafeBuffer("0\r\n\r\n".getBytes(US_ASCII));

    private static final String8FW HEADER_AUTHORITY = new String8FW(":authority");
    private static final String8FW HEADER_CONNECTION = new String8FW("connection");
    private static final String8FW HEADER_METHOD = new String8FW(":method");
    private static final String8FW HEADER_PATH = new String8FW(":path");
    private static final String8FW HEADER_RETRY_AFTER = new String8FW("retry-after");
    private static final String8FW HEADER_STATUS = new String8FW(":status");
    private static final String8FW HEADER_TRANSFER_ENCODING = new String8FW("transfer-encoding");
    private static final String8FW HEADER_UPGRADE = new String8FW("upgrade");

    private static final String16FW METHOD_GET = new String16FW("GET");
    private static final String16FW PATH_SLASH = new String16FW("/");
    private static final String16FW RETRY_AFTER_0 = new String16FW("0");
    private static final String16FW STATUS_101 = new String16FW("101");
    private static final String16FW STATUS_503 = new String16FW("503");
    private static final String16FW TRANSFER_ENCODING_CHUNKED = new String16FW("chunked");

    private static final OctetsFW EMPTY_OCTETS = new OctetsFW().wrap(new UnsafeBuffer(new byte[0]), 0, 0);
    private static final Array32FW<HttpHeaderFW> DEFAULT_HEADERS =
            new Array32FW.Builder<>(new HttpHeaderFW.Builder(), new HttpHeaderFW())
                    .wrap(new UnsafeBuffer(new byte[64]), 0, 64)
                    .item(i -> i.name(HEADER_METHOD).value(METHOD_GET))
                    .item(i -> i.name(HEADER_PATH).value(PATH_SLASH))
                    .build();
    private static final Array32FW<HttpHeaderFW> DEFAULT_TRAILERS =
            new Array32FW.Builder<>(new HttpHeaderFW.Builder(), new HttpHeaderFW())
                         .wrap(new UnsafeBuffer(new byte[8]), 0, 8)
                         .build();
    private static final Array32FW<HttpHeaderFW> EMPTY_OVERRIDES =
            new Array32FW.Builder<>(new HttpHeaderFW.Builder(), new HttpHeaderFW())
                    .wrap(new UnsafeBuffer(new byte[8]), 0, 8)
                    .build();

    private final RouteFW routeRO = new RouteFW();
    private final HttpRouteExFW routeExRO = new HttpRouteExFW();

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final AbortFW abortRO = new AbortFW();
    private final FlushFW flushRO = new FlushFW();

    private final HttpBeginExFW beginExRO = new HttpBeginExFW();
    private final HttpEndExFW endExRO = new HttpEndExFW();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final FlushFW.Builder flushRW = new FlushFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private AbortFW.Builder abortRW = new AbortFW.Builder();

    private final HttpBeginExFW.Builder beginExRW = new HttpBeginExFW.Builder();
    private final HttpEndExFW.Builder endExRW = new HttpEndExFW.Builder();

    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();

    private final AsciiSequenceView asciiRO = new AsciiSequenceView();

    private final HttpClientDecoder decodeHeaders = this::decodeHeaders;
    private final HttpClientDecoder decodeHeadersOnly = this::decodeHeadersOnly;
    private final HttpClientDecoder decodeChunkHeader = this::decodeChunkHeader;
    private final HttpClientDecoder decodeChunkBody = this::decodeChunkBody;
    private final HttpClientDecoder decodeChunkEnd = this::decodeChunkEnd;
    private final HttpClientDecoder decodeContent = this::decodeContent;
    private final HttpClientDecoder decodeTrailers = this::decodeTrailers;
    private final HttpClientDecoder decodeEmptyLines = this::decodeEmptyLines;
    private final HttpClientDecoder decodeUpgraded = this::decodeUpgraded;
    private final HttpClientDecoder decodeIgnore = this::decodeIgnore;

    private final MessageFunction<RouteFW> wrapRoute = (t, b, i, l) -> routeRO.wrap(b, i, i + l);

    private final MutableInteger codecOffset = new MutableInteger();

    private final RouteManager router;
    private final MutableDirectBuffer writeBuffer;
    private final MutableDirectBuffer codecBuffer;
    private final BufferPool bufferPool;
    private final LongUnaryOperator supplyInitialId;
    private final LongUnaryOperator supplyReplyId;
    private final LongSupplier supplyTraceId;
    private final int httpTypeId;
    private final Long2ObjectHashMap<HttpClientPool> clientPools;
    private final Long2ObjectHashMap<MessageConsumer> correlations;
    private final Matcher responseLine;
    private final Matcher versionPart;
    private final Matcher headerLine;
    private final Matcher connectionClose;
    private final int maximumHeadersSize;
    private final int decodeMax;

    private final int maximumQueuedRequestsPerRoute;
    private final int maximumConnectionsPerRoute;
    private final LongSupplier countRequests;
    private final LongSupplier countRequestsRejected;
    private final LongSupplier countRequestsAbandoned;
    private final LongSupplier countResponses;
    private final LongSupplier countResponsesAbandoned;
    private final LongSupplier enqueues;
    private final LongSupplier dequeues;
    private final LongConsumer connectionInUse;

    public HttpClientFactory(
        HttpConfiguration config,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        LongUnaryOperator supplyInitialId,
        LongUnaryOperator supplyReplyId,
        LongSupplier supplyTraceId,
        ToIntFunction<String> supplyTypeId,
        Function<String, LongSupplier> supplyCounter,
        Function<String, LongConsumer> supplyAccumulator)
    {
        this.router = requireNonNull(router);
        this.writeBuffer = requireNonNull(writeBuffer);
        this.codecBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
        this.bufferPool = requireNonNull(bufferPool);
        this.supplyInitialId = requireNonNull(supplyInitialId);
        this.supplyReplyId = requireNonNull(supplyReplyId);
        this.supplyTraceId = requireNonNull(supplyTraceId);
        this.httpTypeId = supplyTypeId.applyAsInt(HttpNukleus.NAME);
        this.correlations = new Long2ObjectHashMap<>();
        this.responseLine = RESPONSE_LINE_PATTERN.matcher("");
        this.headerLine = HEADER_LINE_PATTERN.matcher("");
        this.versionPart = VERSION_PATTERN.matcher("");
        this.connectionClose = CONNECTION_CLOSE_PATTERN.matcher("");
        this.maximumHeadersSize = bufferPool.slotCapacity();

        this.clientPools = new Long2ObjectHashMap<>();
        this.maximumConnectionsPerRoute = config.maximumConnectionsPerRoute();
        this.maximumQueuedRequestsPerRoute = config.maximumRequestsQueuedPerRoute();
        this.countRequests = supplyCounter.apply("http.requests");
        this.countRequestsRejected = supplyCounter.apply("http.requests.rejected");
        this.countRequestsAbandoned = supplyCounter.apply("http.requests.abandoned");
        this.countResponses = supplyCounter.apply("http.responses");
        this.countResponsesAbandoned = supplyCounter.apply("http.responses.abandoned");
        this.enqueues = supplyCounter.apply("http.enqueues");
        this.dequeues = supplyCounter.apply("http.dequeues");
        this.connectionInUse = supplyAccumulator.apply("http.connections.in.use");
        this.decodeMax = bufferPool.slotCapacity();
    }

    @Override
    public MessageConsumer newStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length,
        MessageConsumer sender)
    {
        final BeginFW begin = beginRO.wrap(buffer, index, index + length);
        final long streamId = begin.streamId();

        MessageConsumer newStream;

        if ((streamId & 0x0000_0000_0000_0001L) != 0L)
        {
            newStream = newApplicationStream(begin, sender);
        }
        else
        {
            newStream = correlations.remove(streamId);
        }

        return newStream;
    }

    private MessageConsumer newApplicationStream(
        final BeginFW begin,
        final MessageConsumer application)
    {
        final long routeId = begin.routeId();
        final long authorization = begin.authorization();
        final HttpBeginExFW beginEx = begin.extension().get(beginExRO::tryWrap);

        // TODO: avoid object creation
        final Map<String, String> headers = beginEx != null ? asHeadersMap(beginEx.headers()) : EMPTY_HEADERS;
        final MessagePredicate filter = (t, b, o, l) ->
        {
            final RouteFW route = routeRO.wrap(b, o, o + l);
            final HttpRouteExFW routeEx = route.extension().get(routeExRO::tryWrap);

            boolean headersMatch = true;
            if (routeEx != null)
            {
                headersMatch = !routeEx.headers().anyMatch(
                    h -> !Objects.equals(h.value().asString(), headers.get(h.name().asString())));
            }

            return headersMatch;
        };

        final RouteFW route = router.resolve(routeId, authorization, filter, wrapRoute);

        MessageConsumer newStream = null;

        if (route != null)
        {
            final long resolvedId = route.correlationId();
            final HttpRouteExFW routeEx = route.extension().get(routeExRO::tryWrap);
            final Array32FW<HttpHeaderFW> overrides = routeEx != null ? routeEx.overrides() : EMPTY_OVERRIDES;

            final HttpClientPool clientPool = clientPools.computeIfAbsent(resolvedId, HttpClientPool::new);
            newStream = clientPool.newStream(application, begin, overrides);
        }

        return newStream;
    }

    private void doBegin(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId,
        long authorization,
        long affinity,
        Flyweight extension)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                     .routeId(routeId)
                                     .streamId(streamId)
                                     .sequence(sequence)
                                     .acknowledge(acknowledge)
                                     .maximum(maximum)
                                     .traceId(traceId)
                                     .authorization(authorization)
                                     .affinity(affinity)
                                     .extension(extension.buffer(), extension.offset(), extension.sizeof())
                                     .build();

        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    private void doFlush(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        OctetsFW extension)
    {
        final FlushFW flush = flushRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                     .routeId(routeId)
                                     .streamId(streamId)
                                     .sequence(sequence)
                                     .acknowledge(acknowledge)
                                     .maximum(maximum)
                                     .traceId(traceId)
                                     .authorization(authorization)
                                     .budgetId(budgetId)
                                     .reserved(reserved)
                                     .extension(extension)
                                     .build();

        receiver.accept(flush.typeId(), flush.buffer(), flush.offset(), flush.sizeof());
    }

    private void doData(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int index,
        int length,
        Flyweight extension)
    {
        final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                  .routeId(routeId)
                                  .streamId(streamId)
                                  .sequence(sequence)
                                  .acknowledge(acknowledge)
                                  .maximum(maximum)
                                  .traceId(traceId)
                                  .authorization(authorization)
                                  .budgetId(budgetId)
                                  .reserved(reserved)
                                  .payload(buffer, index, length)
                                  .extension(extension.buffer(), extension.offset(), extension.sizeof())
                                  .build();

        receiver.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    private void doEnd(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId,
        long authorization,
        Flyweight extension)
    {
        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                               .routeId(routeId)
                               .streamId(streamId)
                               .sequence(sequence)
                               .acknowledge(acknowledge)
                               .maximum(maximum)
                               .traceId(traceId)
                               .authorization(authorization)
                               .extension(extension.buffer(), extension.offset(), extension.sizeof())
                               .build();

        receiver.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    private void doAbort(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId,
        long authorization,
        Flyweight extension)
    {
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                     .routeId(routeId)
                                     .streamId(streamId)
                                     .sequence(sequence)
                                     .acknowledge(acknowledge)
                                     .maximum(maximum)
                                     .traceId(traceId)
                                     .authorization(authorization)
                                     .extension(extension.buffer(), extension.offset(), extension.sizeof())
                                     .build();

        receiver.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    private void doReset(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId,
        long authorization)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                     .routeId(routeId)
                                     .streamId(streamId)
                                     .sequence(sequence)
                                     .acknowledge(acknowledge)
                                     .maximum(maximum)
                                     .traceId(traceId)
                                     .authorization(authorization)
                                     .build();

        receiver.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }

    private void doWindow(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long sequence,
        long acknowledge,
        int maximum,
        long traceId,
        long authorization,
        long budgetId,
        int padding)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                        .routeId(routeId)
                                        .streamId(streamId)
                                        .sequence(sequence)
                                        .acknowledge(acknowledge)
                                        .maximum(maximum)
                                        .traceId(traceId)
                                        .authorization(authorization)
                                        .budgetId(budgetId)
                                        .padding(padding)
                                        .build();

        receiver.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    private int decodeHeaders(
        HttpClient client,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        int progress = offset;

        final int endOfHeadersAt = limitOfBytes(buffer, offset, limit, CRLFCRLF_BYTES);

        decode:
        if (endOfHeadersAt != -1)
        {
            final HttpBeginExFW.Builder httpBeginEx = beginExRW.wrap(codecBuffer, 0, codecBuffer.capacity())
                                                               .typeId(httpTypeId);

            final int endOfStartAt = limitOfBytes(buffer, offset, limit, CRLF_BYTES);

            if (endOfStartAt != -1)
            {
                final String status = decodeStartLine(buffer, offset, endOfStartAt);
                if (status == null)
                {
                    client.onDecodeHeadersError(traceId, authorization);
                    client.decoder = decodeIgnore;
                    break decode;
                }

                httpBeginEx.headersItem(h -> h.name(HEADER_STATUS).value(status));

                client.decoder = decodeHeadersOnly;

                final int endOfHeaderLinesAt = endOfHeadersAt - CRLF_BYTES.length;
                int startOfLineAt = endOfStartAt;
                for (int endOfLineAt = limitOfBytes(buffer, startOfLineAt, endOfHeaderLinesAt, CRLF_BYTES);
                        endOfLineAt != -1;
                        startOfLineAt = endOfLineAt,
                                endOfLineAt = limitOfBytes(buffer, startOfLineAt, endOfHeaderLinesAt, CRLF_BYTES))
                {
                    final AsciiSequenceView ascii = asciiRO.wrap(buffer, startOfLineAt, endOfLineAt - startOfLineAt);
                    if (!headerLine.reset(ascii).matches())
                    {
                        client.onDecodeHeadersError(traceId, authorization);
                        client.decoder = decodeIgnore;
                        break decode;
                    }

                    final String name = headerLine.group("name").toLowerCase();
                    final String value = headerLine.group("value");

                    switch (name)
                    {
                    case "content-length":
                        assert client.decoder == decodeHeadersOnly;
                        final int contentLength = parseInt(value);
                        if (contentLength > 0)
                        {
                            client.decodableContentLength = contentLength;
                            client.decoder = decodeContent;
                        }
                        break;

                    case "transfer-encoding":
                        assert client.decoder == decodeHeadersOnly;
                        if ("chunked".equals(value))
                        {
                            client.decoder = decodeChunkHeader;
                        }
                        break;

                    case "upgrade":
                        assert client.decoder == decodeHeadersOnly;
                        if ("101".equals(status))
                        {
                            client.decoder = decodeUpgraded;
                        }
                        break;
                    }

                    httpBeginEx.headersItem(h -> h.name(name).value(value));
                }

                client.onDecodeHeaders(traceId, authorization, httpBeginEx.build());

                progress = endOfHeadersAt;
            }
        }
        else if (limit - offset >= maximumHeadersSize)
        {
            client.decoder = decodeIgnore;
        }

        return progress;
    }

    private String decodeStartLine(
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        final CharSequence startLine = new AsciiSequenceView(buffer, offset, limit - offset);

        return startLine.length() < maximumHeadersSize &&
                responseLine.reset(startLine).matches() &&
                versionPart.reset(responseLine.group("version")).matches() ? responseLine.group("status") : null;
    }

    private int decodeHeadersOnly(
        HttpClient client,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        client.onDecodeHeadersOnly(traceId, authorization, EMPTY_OCTETS);
        client.decoder = decodeEmptyLines;
        return offset;
    }

    private int decodeChunkHeader(
        HttpClient client,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        int progress = offset;

        final int chunkHeaderLimit = limitOfBytes(buffer, offset, limit, CRLF_BYTES);
        if (chunkHeaderLimit != -1)
        {
            final int semicolonAt = limitOfBytes(buffer, offset, chunkHeaderLimit, SEMICOLON_BYTES);
            final int chunkSizeLimit = semicolonAt == -1 ? chunkHeaderLimit - 2 : semicolonAt - 1;
            final int chunkSizeLength = chunkSizeLimit - offset;

            try
            {
                final CharSequence chunkSizeHex = new AsciiSequenceView(buffer, offset, chunkSizeLength);
                client.decodableChunkSize = Integer.parseInt(chunkSizeHex, 0, chunkSizeLength, 16);
                client.decoder = client.decodableChunkSize != 0 ? decodeChunkBody : decodeTrailers;
                progress = chunkHeaderLimit;
            }
            catch (NumberFormatException ex)
            {
                client.onDecodeHeadersError(traceId, authorization);
                client.decoder = decodeIgnore;
            }
        }

        return progress;
    }

    private int decodeChunkBody(
        HttpClient client,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        final int decodableBytes = Math.min(limit - offset, client.decodableChunkSize);

        int progress = offset;
        if (decodableBytes > 0)
        {
            progress = client.onDecodeBody(traceId, authorization, budgetId,
                                           buffer, offset, offset + decodableBytes, EMPTY_OCTETS);
            client.decodableChunkSize -= progress - offset;

            if (client.decodableChunkSize == 0)
            {
                client.decoder = decodeChunkEnd;
            }
        }

        return progress;
    }

    private int decodeChunkEnd(
        HttpClient client,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        int progress = offset;
        if (limit - progress >= 2)
        {
            if (buffer.getByte(offset) != '\r' ||
                buffer.getByte(offset + 1) != '\n')
            {
                client.onDecodeBodyError(traceId, authorization);
                client.decoder = decodeIgnore;
            }
            else
            {
                client.decoder = decodeChunkHeader;
                progress += 2;
            }
        }
        return progress;
    }

    private int decodeContent(
        HttpClient client,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        final int length = Math.min(limit - offset, client.decodableContentLength);

        int progress = offset;
        if (length > 0)
        {
            progress = client.onDecodeBody(traceId, authorization, budgetId, buffer, offset, offset + length, EMPTY_OCTETS);
            client.decodableContentLength -= progress - offset;
        }

        assert client.decodableContentLength >= 0;

        if (client.decodableContentLength == 0)
        {
            client.onDecodeTrailers(traceId, authorization, EMPTY_OCTETS);
            client.decoder = decodeEmptyLines;
        }

        return progress;
    }

    private int decodeTrailers(
        HttpClient client,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        int progress = offset;

        final int endOfTrailersAt = limitOfBytes(buffer, offset, limit, CRLFCRLF_BYTES);
        if (endOfTrailersAt != -1)
        {
            // TODO
            final HttpEndExFW httpEndEx = endExRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                                 .typeId(httpTypeId)
                                                 .build();

            client.onDecodeTrailers(traceId, authorization, httpEndEx);
            progress = endOfTrailersAt;
            client.decoder = decodeEmptyLines;
        }
        else if (buffer.getByte(offset) == '\r' &&
            buffer.getByte(offset + 1) == '\n')
        {
            client.onDecodeTrailers(traceId, authorization, EMPTY_OCTETS);
            progress += 2;
            client.decoder = decodeEmptyLines;
        }

        return progress;
    }

    private int decodeEmptyLines(
        HttpClient client,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        int progress = offset;
        if (limit - progress >= 2)
        {
            if (buffer.getByte(offset) == '\r' &&
                buffer.getByte(offset + 1) == '\n')
            {
                progress += 2;
            }
            else
            {
                client.decoder = decodeHeaders;
            }
        }
        return progress;
    }

    private int decodeUpgraded(
        HttpClient client,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        return client.onDecodeBody(traceId, authorization, budgetId, buffer, offset, limit, EMPTY_OCTETS);
    }

    private int decodeIgnore(
        HttpClient client,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        client.doNetworkWindow(traceId, authorization, budgetId, reserved, 0);
        return limit;
    }

    @FunctionalInterface
    private interface HttpClientDecoder
    {
        int decode(
            HttpClient client,
            long traceId,
            long authorization,
            long budgetId,
            int reserved,
            DirectBuffer buffer,
            int offset,
            int limit);
    }

    private final class HttpClientPool
    {
        private final long resolvedId;
        private final List<HttpClient> clients;
        private final Queue<HttpRequest> requests;

        private HttpClientPool(
            long resolvedId)
        {
            this.resolvedId = resolvedId;
            this.clients = new LinkedList<>();
            this.requests = new LinkedList<>();
        }

        public MessageConsumer newStream(
            MessageConsumer sender,
            BeginFW begin,
            Array32FW<HttpHeaderFW> overrides)
        {
            // count all requests
            countRequests.getAsLong();

            HttpClient client = supplyClient();

            MessageConsumer newStream = null;

            if (client != null)
            {
                final HttpExchange exchange = client.newExchange(sender, begin, overrides);
                newStream = exchange::onApplication;
            }
            else if (requests.size() < maximumQueuedRequestsPerRoute)
            {
                final HttpRequest request = new HttpRequest(sender, overrides);
                requests.offer(request);

                enqueues.getAsLong();

                newStream = request::onApplication;
            }
            else
            {
                // count all responses
                countResponses.getAsLong();

                final long sequence = begin.sequence();
                final long acknowledge = begin.acknowledge();
                final int maximum = begin.maximum();
                final long routeId = begin.routeId();
                final long initialId = begin.streamId();
                final long traceId = begin.traceId();
                final long authorization = begin.authorization();
                final long replyId = supplyReplyId.applyAsLong(initialId);

                doWindow(sender, routeId, initialId, client.replySeq, client.replyAck, 0, traceId, authorization, 0L, 0);

                HttpBeginExFW beginEx = beginExRW.wrap(codecBuffer, 0, codecBuffer.capacity())
                        .typeId(httpTypeId)
                        .headersItem(h -> h.name(HEADER_STATUS).value(STATUS_503))
                        .headersItem(h -> h.name(HEADER_RETRY_AFTER).value(RETRY_AFTER_0))
                        .build();
                doBegin(sender, routeId, replyId, sequence, acknowledge, maximum, supplyTraceId.getAsLong(), 0L, 0, beginEx);
                doEnd(sender, routeId, replyId, sequence, acknowledge, maximum, supplyTraceId.getAsLong(), 0, EMPTY_OCTETS);

                // count rejected requests (no connection or no space in the queue)
                countRequestsRejected.getAsLong();

                // ignore DATA, FLUSH, END, ABORT
                newStream = (t, b, i, l) -> {};
            }

            assert requests.size() <= maximumQueuedRequestsPerRoute;

            return newStream;
        }

        private void flushNext()
        {
            if (!requests.isEmpty())
            {
                HttpClient client = supplyClient();

                if (client != null)
                {
                    final HttpRequest request = requests.poll();
                    assert request != null;

                    dequeues.getAsLong();

                    request.doFlushBegin(client);
                }
            }
        }

        private HttpClient supplyClient()
        {
            HttpClient client = clients.stream().filter(c -> c.exchange == null).findFirst().orElse(null);

            if (client == null && clients.size() < maximumConnectionsPerRoute)
            {
                client = new HttpClient(this);
                onCreated(client);
            }

            return client;
        }

        private void onCreated(
            HttpClient client)
        {
            if (clients.add(client))
            {
                connectionInUse.accept(1L);
            }

            assert clients.size() <= maximumConnectionsPerRoute;
        }

        private void onUpgradedOrClosed(
            HttpClient client)
        {
            if (clients.remove(client))
            {
                connectionInUse.accept(-1L);
            }

            assert clients.size() <= maximumConnectionsPerRoute;
        }

        private final class HttpRequest
        {
            private final MessageConsumer sender;
            private final Array32FW<HttpHeaderFW> overrides;
            private MessageConsumer receiver;
            private DirectBuffer message;

            private HttpRequest(
                MessageConsumer sender,
                Array32FW<HttpHeaderFW> overrides)
            {
                this.sender = sender;
                this.overrides = overrides;
                this.receiver = this::onQueuedMessage;
            }

            private void doFlushBegin(
                HttpClient client)
            {
                final BeginFW begin = beginRO.wrap(message, 0, message.capacity());
                final HttpExchange exchange = client.newExchange(sender, begin, overrides);

                this.receiver = exchange::onApplication;
                this.message = null;

                onApplication(BeginFW.TYPE_ID, begin.buffer(), begin.offset(), begin.sizeof());
            }

            private void onApplication(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
            {
                receiver.accept(msgTypeId, buffer, index, length);
            }

            private void onQueuedMessage(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
            {
                switch (msgTypeId)
                {
                case BeginFW.TYPE_ID:
                    assert message == null;
                    byte[] bytes = new byte[length];
                    buffer.getBytes(index, bytes, 0, length);
                    message = new UnsafeBuffer(bytes);
                    break;
                case AbortFW.TYPE_ID:
                    requests.remove(this);
                    dequeues.getAsLong();
                    break;
                }
            }
        }
    }

    private final class HttpClient
    {
        private final HttpClientPool pool;
        private final MessageConsumer network;
        private final long routeId;
        private final long initialId;
        private final long replyId;

        private int decodeSlot;
        private int decodeSlotOffset;
        private int decodeSlotReserved;
        private long decodeSlotBudgetId;

        private int encodeSlot;
        private int encodeSlotOffset;

        private HttpClientDecoder decoder;
        private int decodableChunkSize;
        private int decodableContentLength;

        private HttpExchange exchange;
        private int state;
        private long initialSeq;
        private long initialAck;
        private int initialMax;
        private int initialPad;

        private long replySeq;
        private long replyAck;
        private long replyAuth;

        private HttpClient(
            HttpClientPool pool)
        {
            this.pool = pool;
            this.routeId = pool.resolvedId;
            this.initialId = supplyInitialId.applyAsLong(routeId);
            this.network = router.supplyReceiver(initialId);
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.decoder = decodeEmptyLines;
            this.decodeSlot = NO_SLOT;
            this.encodeSlot = NO_SLOT;
        }

        public int initialPendingAck()
        {
            return (int)(initialSeq - initialAck) + encodeSlotOffset;
        }

        private int initialWindow()
        {
            return initialMax - initialPendingAck();
        }

        private HttpExchange newExchange(
            MessageConsumer sender,
            BeginFW begin,
            Array32FW<HttpHeaderFW> overrides)
        {
            final long routeId = begin.routeId();
            final long initialId = begin.streamId();

            return new HttpExchange(this, sender, routeId, initialId, overrides);
        }

        private void onNetwork(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                onNetworkBegin(begin);
                break;
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                onNetworkData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                onNetworkEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onNetworkAbort(abort);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                onNetworkReset(reset);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                onNetworkWindow(window);
                break;
            }
        }

        private void onNetworkBegin(
            BeginFW begin)
        {
            final long traceId = begin.traceId();
            final long authorization = begin.authorization();
            replyAuth = authorization;

            doNetworkWindow(traceId, authorization, 0, 0, bufferPool.slotCapacity());
        }

        private void onNetworkData(
            DataFW data)
        {
            final long sequence = data.sequence();
            final long acknowledge = data.acknowledge();
            final long traceId = data.traceId();
            final long authorization = data.authorization();
            final long budgetId = data.budgetId();

            assert acknowledge <= sequence;
            assert sequence >= replySeq;
            assert acknowledge <= replyAck;

            replySeq = sequence + data.reserved();
            replyAuth = authorization;

            assert replyAck <= replySeq;

            if (replySeq > replyAck + decodeMax)
            {
                cleanupNetwork(traceId, authorization);
            }
            else
            {
                final OctetsFW payload = data.payload();
                int reserved = data.reserved();
                DirectBuffer buffer = payload.buffer();
                int offset = payload.offset();
                int limit = payload.limit();

                if (decodeSlot != NO_SLOT)
                {
                    final MutableDirectBuffer slotBuffer = bufferPool.buffer(decodeSlot);
                    slotBuffer.putBytes(decodeSlotOffset, buffer, offset, limit - offset);
                    decodeSlotOffset += limit - offset;
                    decodeSlotReserved += reserved;
                    decodeSlotBudgetId = budgetId;
                    buffer = slotBuffer;
                    offset = 0;
                    limit = decodeSlotOffset;
                    reserved = decodeSlotReserved;
                }

                decodeNetwork(traceId, authorization, budgetId, reserved, buffer, offset, limit);
            }
        }

        private void onNetworkEnd(
            EndFW end)
        {
            final long traceId = end.traceId();
            final long authorization = end.authorization();

            state = HttpState.closingReply(state);

            if (exchange != null && !HttpState.replyOpening(exchange.state) ||
                decodeSlot == NO_SLOT)
            {
                state = HttpState.closeReply(state);

                if (exchange != null)
                {
                    exchange.cleanup(traceId, authorization);
                    cleanupDecodeSlotIfNecessary();
                }

                doNetworkEnd(traceId, authorization);
            }
        }

        private void onNetworkAbort(
            AbortFW abort)
        {
            final long traceId = abort.traceId();
            final long authorization = abort.authorization();

            state = HttpState.closeReply(state);

            cleanupDecodeSlotIfNecessary();

            if (exchange != null)
            {
                exchange.cleanup(traceId, authorization);
            }

            doNetworkAbort(traceId, authorization);
        }

        private void onNetworkReset(
            ResetFW reset)
        {
            final long traceId = reset.traceId();
            final long authorization = reset.authorization();

            state = HttpState.closeInitial(state);

            cleanupEncodeSlotIfNecessary();

            if (exchange != null)
            {
                exchange.cleanup(traceId, authorization);
            }

            doNetworkReset(traceId, authorization);
        }

        private void onNetworkWindow(
            WindowFW window)
        {
            final long sequence = window.sequence();
            final long acknowledge = window.acknowledge();
            final long traceId = window.traceId();
            final long authorization = window.authorization();
            final long budgetId = window.budgetId();
            final int maximum = window.maximum();
            final int padding = window.padding();

            assert acknowledge <= sequence;
            assert sequence <= initialSeq;
            assert acknowledge >= initialAck;
            assert maximum >= initialMax;

            initialAck = acknowledge;
            initialMax = maximum;
            initialPad = padding;

            flushNetworkIfBuffered(traceId, authorization, budgetId);

            if (exchange != null && !HttpState.initialClosed(exchange.state))
            {
                exchange.flushRequestWindow(traceId, budgetId);
            }
        }

        private void flushNetworkIfBuffered(
            long traceId,
            long authorization,
            long budgetId)
        {
            if (encodeSlot != NO_SLOT)
            {
                final MutableDirectBuffer buffer = bufferPool.buffer(encodeSlot);
                final int limit = encodeSlotOffset;
                final int reserved = limit + initialPad;
                doNetworkData(traceId, authorization, budgetId, reserved, buffer, 0, limit);
            }
        }

        private void doNetworkBegin(
            long traceId,
            long authorization,
            long affinity)
        {
            if (!HttpState.initialOpening(state))
            {
                state = HttpState.openingInitial(state);

                doBegin(network, routeId, initialId, initialSeq, initialAck,
                    initialMax, traceId, authorization, affinity, EMPTY_OCTETS);
                router.setThrottle(initialId, this::onNetwork);
                correlations.put(replyId, this::onNetwork);
            }
        }

        private void doNetFlush(
            long traceId,
            long budgetId,
            int reserved,
            OctetsFW extension)
        {
            doFlush(network, routeId, initialId, initialSeq, initialAck,
                initialMax, traceId, replyAuth, budgetId, reserved, extension);
        }

        private void doNetworkData(
            long traceId,
            long authorization,
            long budgetId,
            int reserved,
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            final int maxLength = limit - offset;
            final int length = Math.max(Math.min(initialWindow() - initialPad, maxLength), 0);

            if (length > 0)
            {
                final int required = length + initialPad;

                assert reserved >= required;

                doData(network, routeId, initialId, initialSeq, initialAck, initialMax, traceId, authorization, budgetId,
                       required, buffer, offset, length, EMPTY_OCTETS);

                initialSeq += required;

                assert initialSeq <= initialAck + initialMax :
                    String.format("%d <= %d + %d", initialSeq, initialAck, initialMax);
            }

            final int remaining = maxLength - length;
            if (remaining > 0)
            {
                if (encodeSlot == NO_SLOT)
                {
                    encodeSlot = bufferPool.acquire(replyId);
                }

                if (encodeSlot == NO_SLOT)
                {
                    cleanupNetwork(traceId, authorization);
                }
                else
                {
                    final MutableDirectBuffer encodeBuffer = bufferPool.buffer(encodeSlot);
                    encodeBuffer.putBytes(0, buffer, offset + length, remaining);
                    encodeSlotOffset = remaining;
                }
            }
            else
            {
                cleanupEncodeSlotIfNecessary();
            }
        }

        private void doNetworkEnd(
            long traceId,
            long authorization)
        {
            if (!HttpState.initialClosed(state))
            {
                state = HttpState.closeInitial(state);
                cleanupEncodeSlotIfNecessary();
                doEnd(network, routeId, initialId, initialSeq, initialAck, initialMax, traceId, authorization, EMPTY_OCTETS);

                if (HttpState.closed(state))
                {
                    pool.onUpgradedOrClosed(this);
                }
            }
        }

        private void doNetworkAbort(
            long traceId,
            long authorization)
        {
            if (!HttpState.initialClosed(state))
            {
                state = HttpState.closeInitial(state);
                cleanupEncodeSlotIfNecessary();
                doAbort(network, routeId, initialId, initialSeq, initialAck, initialMax, traceId, authorization, EMPTY_OCTETS);

                if (HttpState.closed(state))
                {
                    pool.onUpgradedOrClosed(this);
                }
            }
        }

        private void doNetworkReset(
            long traceId,
            long authorization)
        {
            if (!HttpState.replyClosed(state))
            {
                state = HttpState.closeReply(state);
                cleanupDecodeSlotIfNecessary();
                correlations.remove(replyId);
                doReset(network, routeId, replyId, replySeq, replyAck, initialMax, traceId, authorization);

                if (HttpState.closed(state))
                {
                    pool.onUpgradedOrClosed(this);
                }
            }
        }

        private void flushNetworkWindow(
            long traceId,
            long budgetId,
            int replyPad)
        {
            final int replyMax = exchange != null ? decodeMax : 0;
            final int decodable = decodeMax - replyMax;

            final long replyAckMax = Math.min(replyAck + decodable, replySeq);
            if (replyAckMax > replyAck)
            {
                replyAck = replyAckMax;
                assert replyAck <= replySeq;

                doNetworkWindow(traceId, replyAuth, budgetId, replyPad, decodeMax);
            }
        }

        private void doNetworkWindow(
            long traceId,
            long authorization,
            long budgetId,
            int padding,
            int maximum)
        {
            doWindow(network, routeId, replyId,  replySeq, replyAck, maximum, traceId, authorization, budgetId, padding);
        }

        private void decodeNetworkIfBuffered(
            long traceId,
            long authorization)
        {
            if (decodeSlot != NO_SLOT)
            {
                final MutableDirectBuffer decodeBuffer = bufferPool.buffer(decodeSlot);
                final int decodeLength = decodeSlotOffset;
                decodeNetwork(traceId, authorization, decodeSlotBudgetId, decodeSlotReserved, decodeBuffer, 0, decodeLength);
            }
        }

        private void decodeNetwork(
            long traceId,
            long authorization,
            long budgetId,
            int reserved,
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            HttpClientDecoder previous = null;
            int progress = offset;
            while (progress <= limit && previous != decoder)
            {
                previous = decoder;
                progress = decoder.decode(this, traceId, authorization, budgetId, reserved, buffer, progress, limit);
            }

            if (progress < limit)
            {
                if (decodeSlot == NO_SLOT)
                {
                    decodeSlot = bufferPool.acquire(initialId);
                }

                if (decodeSlot == NO_SLOT)
                {
                    cleanupNetwork(traceId, authorization);
                }
                else
                {
                    final MutableDirectBuffer decodeBuffer = bufferPool.buffer(decodeSlot);
                    decodeBuffer.putBytes(0, buffer, progress, limit - progress);
                    decodeSlotOffset = limit - progress;
                }
            }
            else
            {
                cleanupDecodeSlotIfNecessary();

                if (decoder == decodeIgnore)
                {
                    cleanupNetwork(traceId, authorization);
                }
                else if (HttpState.replyClosing(state))
                {
                    state = HttpState.closeReply(state);

                    if (exchange != null)
                    {
                        exchange.cleanup(traceId, authorization);
                    }

                    doNetworkEnd(traceId, authorization);
                }
            }
        }

        private void onDecodeHeadersError(
            long traceId,
            long authorization)
        {
            cleanupNetwork(traceId, authorization);
        }

        private void onDecodeBodyError(
            long traceId,
            long authorization)
        {
            cleanupNetwork(traceId, authorization);
        }

        private void onDecodeHeaders(
            long traceId,
            long authorization,
            HttpBeginExFW beginEx)
        {
            exchange.doResponseBegin(traceId, authorization, beginEx);

            final HttpHeaderFW connection = beginEx.headers().matchFirst(h -> HEADER_CONNECTION.equals(h.name()));
            if (connection != null && connectionClose.reset(connection.value().asString()).matches())
            {
                exchange.state = HttpState.closingReply(exchange.state);
            }

            final HttpHeaderFW status = beginEx.headers().matchFirst(h -> HEADER_STATUS.equals(h.name()));
            if (status != null && STATUS_101.equals(status.value()))
            {
                pool.onUpgradedOrClosed(this);
            }
        }

        private void onDecodeHeadersOnly(
            long traceId,
            long authorization,
            Flyweight extension)
        {
            exchange.doResponseEnd(traceId, authorization, extension);
        }

        private int onDecodeBody(
            long traceId,
            long authorization,
            long budgetId,
            DirectBuffer buffer,
            int offset,
            int limit,
            Flyweight extension)
        {
            return exchange.doResponseData(traceId, authorization, budgetId, buffer, offset, limit, extension);
        }

        private void onDecodeTrailers(
            long traceId,
            long authorization,
            Flyweight extension)
        {
            exchange.doResponseEnd(traceId, authorization, extension);
        }

        private void doEncodeHeaders(
            HttpExchange exchange,
            long traceId,
            long authorization,
            long budgetId,
            Array32FW<HttpHeaderFW> headers,
            Array32FW<HttpHeaderFW> overrides)
        {
            assert exchange == this.exchange;

            Map<String8FW, String16FW> headersMap = new LinkedHashMap<>();
            headers.forEach(h -> headersMap.put(newString8FW(h.name()), newString16FW(h.value())));
            overrides.forEach(h -> headersMap.put(newString8FW(h.name()), newString16FW(h.value())));

            final String16FW transferEncoding = headersMap.get(HEADER_TRANSFER_ENCODING);
            exchange.requestChunked = transferEncoding != null && TRANSFER_ENCODING_CHUNKED.equals(transferEncoding);

            final String16FW connection = headersMap.get(HEADER_CONNECTION);
            final String16FW upgrade = headersMap.get(HEADER_UPGRADE);

            if (connection != null && connectionClose.reset(connection.asString()).matches() ||
                upgrade != null)
            {
                exchange.state = HttpState.closingReply(exchange.state);
            }

            codecOffset.value = doEncodeStart(codecBuffer, 0, headersMap);
            codecOffset.value = doEncodeHost(codecBuffer, codecOffset.value, headersMap);
            headersMap.forEach((n, v) -> codecOffset.value = doEncodeHeader(codecBuffer, codecOffset.value, n, v));
            codecBuffer.putBytes(codecOffset.value, CRLF_BYTES);
            codecOffset.value += CRLF_BYTES.length;

            final int length = codecOffset.value;

            if (length > maximumHeadersSize)
            {
                exchange.doRequestReset(traceId, authorization);
                doNetworkAbort(traceId, authorization);
            }
            else
            {
                final int reserved = length + initialPad;
                doNetworkData(traceId, authorization, budgetId, reserved, codecBuffer, 0, length);
            }
        }

        private int doEncodeHost(
            MutableDirectBuffer buffer,
            int offset,
            Map<String8FW, String16FW> headersMap)
        {
            int progress = offset;

            final String16FW authority = headersMap.get(HEADER_AUTHORITY);
            if (authority != null)
            {
                final DirectBuffer authorityValue = authority.value();

                codecBuffer.putBytes(progress, HOST_BYTES);
                progress += HOST_BYTES.length;
                codecBuffer.putBytes(progress, COLON_SPACE_BYTES);
                progress += COLON_SPACE_BYTES.length;
                codecBuffer.putBytes(progress, authorityValue, 0, authorityValue.capacity());
                progress += authorityValue.capacity();
                codecBuffer.putBytes(progress, CRLF_BYTES);
                progress += CRLF_BYTES.length;
            }

            return progress;
        }

        private int doEncodeStart(
            MutableDirectBuffer buffer,
            int offset,
            Map<String8FW, String16FW> headersMap)
        {
            int progress = offset;

            final DirectBuffer method = headersMap.getOrDefault(HEADER_METHOD, METHOD_GET).value();
            codecBuffer.putBytes(progress, method, 0, method.capacity());
            progress += method.capacity();

            codecBuffer.putByte(progress, SPACE_BYTE);
            progress++;

            final DirectBuffer path = headersMap.getOrDefault(HEADER_PATH, PATH_SLASH).value();
            codecBuffer.putBytes(progress, path, 0, path.capacity());
            progress += path.capacity();

            codecBuffer.putByte(progress, SPACE_BYTE);
            progress++;

            codecBuffer.putBytes(progress, HTTP_1_1_BYTES);
            progress += HTTP_1_1_BYTES.length;

            codecBuffer.putBytes(progress, CRLF_BYTES);
            progress += CRLF_BYTES.length;

            return progress;
        }

        private int doEncodeHeader(
            MutableDirectBuffer buffer,
            int offset,
            String8FW headerName,
            String16FW headerValue)
        {
            int progress = offset;
            final DirectBuffer name = headerName.value();
            if (name.getByte(0) != COLON_BYTE)
            {
                final DirectBuffer value = headerValue.value();

                boolean uppercase = true;
                for (int pos = 0, len = name.capacity(); pos < len; pos++, progress++)
                {
                    byte ch = name.getByte(pos);
                    if (uppercase)
                    {
                        ch = (byte) toUpperCase(ch);
                    }
                    else
                    {
                        ch |= (byte) toLowerCase(ch);
                    }
                    buffer.putByte(progress, ch);
                    uppercase = ch == HYPHEN_BYTE;
                }

                buffer.putBytes(progress, COLON_SPACE_BYTES);
                progress += COLON_SPACE_BYTES.length;
                buffer.putBytes(progress, value, 0, value.capacity());
                progress += value.capacity();
                buffer.putBytes(progress, CRLF_BYTES);
                progress += CRLF_BYTES.length;
            }
            return progress;
        }

        private void doEncodeBody(
            HttpExchange exchange,
            long traceId,
            long authorization,
            int flags,
            long budgetId,
            int reserved,
            OctetsFW payload)
        {
            assert exchange == this.exchange;

            DirectBuffer buffer = payload.buffer();
            int offset = payload.offset();
            int limit = payload.limit();

            if (exchange.requestChunked && flags != 0)
            {
                int chunkLimit = 0;

                if ((flags & 0x01) != 0)
                {
                    final String chunkSizeHex = Integer.toHexString(payload.sizeof());
                    chunkLimit += codecBuffer.putStringWithoutLengthAscii(chunkLimit, chunkSizeHex);
                    codecBuffer.putBytes(chunkLimit, CRLF_BYTES);
                    chunkLimit += 2;
                }

                codecBuffer.putBytes(chunkLimit, payload.buffer(), payload.offset(), payload.sizeof());

                if ((flags & 0x02) != 0)
                {
                    codecBuffer.putBytes(chunkLimit, CRLF_BYTES);
                    chunkLimit += 2;
                }

                buffer = codecBuffer;
                offset = 0;
                limit = chunkLimit;
            }

            doNetworkData(traceId, authorization, budgetId, reserved, buffer, offset, limit);
        }

        private void doEncodeTrailers(
            HttpExchange exchange,
            long traceId,
            long authorization,
            long budgetId,
            Array32FW<HttpHeaderFW> trailers)
        {
            assert exchange == this.exchange;

            if (exchange.requestChunked)
            {
                DirectBuffer buffer = ZERO_CHUNK;
                int offset = 0;
                int limit = ZERO_CHUNK.capacity();

                if (!trailers.isEmpty())
                {
                    codecOffset.value = 0;
                    codecBuffer.putByte(codecOffset.value, ZERO_BYTE);
                    codecOffset.value++;
                    codecBuffer.putBytes(codecOffset.value, CRLF_BYTES);
                    codecOffset.value += CRLF_BYTES.length;
                    trailers.forEach(
                        h -> codecOffset.value = doEncodeHeader(writeBuffer, codecOffset.value, h.name(), h.value()));
                    codecBuffer.putBytes(codecOffset.value, CRLF_BYTES);
                    codecOffset.value += CRLF_BYTES.length;

                    buffer = codecBuffer;
                    offset = 0;
                    limit = codecOffset.value;
                }

                final int reserved = limit + initialPad;
                doNetworkData(traceId, authorization, budgetId, reserved, buffer, offset, limit);
            }

            if (HttpState.closed(exchange.state))
            {
                this.exchange = null;
            }
        }

        private void cleanupNetwork(
            long traceId,
            long authorization)
        {
            doNetworkReset(traceId, authorization);
            doNetworkAbort(traceId, authorization);

            if (exchange != null)
            {
                exchange.cleanup(traceId, authorization);
                exchange = null;
            }
        }

        private void cleanupDecodeSlotIfNecessary()
        {
            if (decodeSlot != NO_SLOT)
            {
                bufferPool.release(decodeSlot);
                decodeSlot = NO_SLOT;
                decodeSlotOffset = 0;
            }
        }

        private void cleanupEncodeSlotIfNecessary()
        {
            if (encodeSlot != NO_SLOT)
            {
                bufferPool.release(encodeSlot);
                encodeSlot = NO_SLOT;
                encodeSlotOffset = 0;
            }
        }
    }


    private final class HttpExchange
    {
        private final HttpClient client;
        private final MessageConsumer application;
        private final long routeId;
        private final long requestId;
        private final long responseId;
        private final Array32FW<HttpHeaderFW> overrides;

        private int requestBudget;
        private int responseBudget;
        private int responsePadding;

        private int state;
        private boolean requestChunked;
        private long initialSeq;
        private long initialAck;
        private long initialAuth;
        private long replyAck;
        private int replyMax;
        private int replyPad;
        private long replySeq;

        private HttpExchange(
            HttpClient client,
            MessageConsumer application,
            long routeId,
            long requestId,
            Array32FW<HttpHeaderFW> overrides)
        {
            this.client = client;
            this.application = application;
            this.routeId = routeId;
            this.requestId = requestId;
            this.responseId = supplyReplyId.applyAsLong(requestId);
            this.overrides = overrides;
        }

        private void onApplication(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                onRequestBegin(begin);
                break;
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                onRequestData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                onRequestEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onRequestAbort(abort);
                break;
            case FlushFW.TYPE_ID:
                final FlushFW flush = flushRO.wrap(buffer, index, index + length);
                onRequestFlush(flush);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                onResponseReset(reset);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                onResponseWindow(window);
                break;
            }
        }

        private void onRequestBegin(
            BeginFW begin)
        {
            final HttpBeginExFW beginEx = begin.extension().get(beginExRO::tryWrap);
            final Array32FW<HttpHeaderFW> headers = beginEx != null ? beginEx.headers() : DEFAULT_HEADERS;

            final long traceId = begin.traceId();
            final long authorization = begin.authorization();
            final long sequence = begin.sequence();
            final long acknowledge = begin.acknowledge();

            assert acknowledge <= sequence;
            assert sequence >= initialSeq;
            assert acknowledge >= initialAck;

            initialSeq = sequence;
            initialAck = acknowledge;
            initialAuth = authorization;

            assert initialAck <= initialSeq;

            assert client.exchange == null;
            client.exchange = this;

            state = HttpState.openingInitial(state);

            client.doNetworkBegin(traceId, authorization, 0);
            client.doEncodeHeaders(this, traceId, authorization, 0L, headers, overrides);
        }

        private void onRequestFlush(
            FlushFW flush)
        {
            final long sequence = flush.sequence();
            final long acknowledge = flush.acknowledge();
            final long traceId = flush.traceId();
            final long authorization = flush.authorization();
            final long budgetId = flush.budgetId();
            final int reserved = flush.reserved();
            final OctetsFW extension = flush.extension();

            assert acknowledge <= sequence;
            assert sequence >= initialSeq;

            initialSeq = sequence;
            initialAuth = authorization;

            assert initialAck <= initialSeq;

            if (initialSeq > initialAck + client.initialMax)
            {
                doRequestReset(traceId, authorization);
                client.doNetworkAbort(traceId, authorization);
            }
            else
            {
                client.doNetFlush(traceId, budgetId, reserved, extension);
            }
        }

        private void onRequestData(
            DataFW data)
        {
            final long sequence = data.sequence();
            final long acknowledge = data.acknowledge();
            final long traceId = data.traceId();
            final long authorization = data.authorization();

            assert acknowledge <= sequence;
            assert sequence >= initialSeq;

            initialSeq = sequence + data.reserved();
            initialAuth = authorization;

            assert initialAck <= initialSeq;

            if (initialSeq > initialAck + client.initialMax)
            {
                doRequestReset(traceId, authorization);
                client.doNetworkAbort(traceId, authorization);
            }
            else
            {
                final int flags = data.flags();
                final long budgetId = data.budgetId();
                final int reserved = data.reserved();
                final OctetsFW payload = data.payload();

                client.doEncodeBody(this, traceId, authorization, flags, budgetId, reserved, payload);
            }
        }

        private void onRequestEnd(
            EndFW end)
        {
            final long sequence = end.sequence();
            final long acknowledge = end.acknowledge();
            final long traceId = end.traceId();
            final long authorization = end.authorization();

            assert acknowledge <= sequence;
            assert sequence >= initialSeq;

            initialSeq = sequence;
            initialAuth = authorization;

            assert initialAck <= initialSeq;

            final HttpEndExFW endEx = end.extension().get(endExRO::tryWrap);
            final Array32FW<HttpHeaderFW> trailers = endEx != null ? endEx.trailers() : DEFAULT_TRAILERS;

            state = HttpState.closeInitial(state);
            client.doEncodeTrailers(this, traceId, authorization, 0L, trailers);
        }

        private void onRequestAbort(
            AbortFW abort)
        {
            final long sequence = abort.sequence();
            final long acknowledge = abort.acknowledge();
            final long traceId = abort.traceId();
            final long authorization = abort.authorization();

            assert acknowledge <= sequence;
            assert sequence >= initialSeq;

            initialSeq = sequence;
            initialAuth = authorization;

            assert initialAck <= initialSeq;

            state = HttpState.closeInitial(state);
            client.doNetworkAbort(traceId, authorization);

            doResponseAbort(traceId, authorization, EMPTY_OCTETS);

            client.doNetworkReset(traceId, authorization);
        }

        private void doRequestReset(
            long traceId,
            long authorization)
        {
            if (!HttpState.initialClosed(state))
            {
                state = HttpState.closeInitial(state);
                doReset(application, routeId, requestId, initialSeq, initialAck, client.initialMax, traceId, authorization);

                if (HttpState.closed(state))
                {
                    onExchangeClosed();
                }
            }
        }

        private void flushRequestWindow(
            long traceId,
            long budgetId)
        {
            // TODO: consider encodePool capacity
            int initialAckMax = (int)(initialSeq - client.initialPendingAck());
            if (initialAckMax > initialAck)
            {
                initialAck = initialAckMax;
                assert initialAck <= initialSeq;

                doRequestWindow(traceId, budgetId);
            }
        }

        private void doRequestWindow(
            long traceId,
            long budgetId)
        {
            state = HttpState.openInitial(state);
            doWindow(application, routeId, requestId,
                initialSeq, initialAck, client.initialMax, traceId, initialAuth, budgetId, client.initialPad);
        }

        private void doResponseBegin(
            long traceId,
            long authorization,
            Flyweight extension)
        {
            // count all responses
            countResponses.getAsLong();

            state = HttpState.openingReply(state);

            doBegin(application, routeId, responseId, replySeq, replyAck, replyMax, traceId, authorization, 0, extension);
            router.setThrottle(responseId, this::onApplication);
        }

        private int doResponseData(
            long traceId,
            long authorization,
            long budgetId,
            DirectBuffer buffer,
            int offset,
            int limit,
            Flyweight extension)
        {
            int length = Math.min(responseBudget - responsePadding, limit - offset);

            if (length > 0)
            {
                final int reserved = length + responsePadding;

                responseBudget -= reserved;

                assert responseBudget >= 0;

                doData(application, routeId, responseId, replySeq, replyAck, replyMax, traceId, authorization, budgetId,
                       reserved, buffer, offset, length, extension);
            }

            return offset + length;
        }

        private void doResponseEnd(
            long traceId,
            long authorization,
            Flyweight extension)
        {
            if (!HttpState.replyClosed(state))
            {
                if (HttpState.replyClosing(state))
                {
                    client.doNetworkEnd(traceId, authorization);
                }

                state = HttpState.closeReply(state);
                doEnd(application, routeId, responseId, replySeq, replyAck, replyMax, traceId, authorization, extension);

                if (HttpState.closed(state))
                {
                    onExchangeClosed();
                }
            }
        }

        private void doResponseAbort(
            long traceId,
            long authorization,
            Flyweight extension)
        {
            if (!HttpState.replyClosed(state))
            {
                if (HttpState.replyOpening(state))
                {
                    state = HttpState.closeReply(state);
                    doAbort(application, routeId, responseId, replySeq, replyAck, replyMax, traceId, authorization, extension);

                    // count abandoned responses
                    countResponsesAbandoned.getAsLong();

                    if (HttpState.closed(state))
                    {
                        onExchangeClosed();
                    }
                }
                else
                {
                    HttpBeginExFW beginEx = beginExRW.wrap(codecBuffer, 0, codecBuffer.capacity())
                            .typeId(httpTypeId)
                            .headersItem(h -> h.name(HEADER_STATUS).value(STATUS_503))
                            .headersItem(h -> h.name(HEADER_RETRY_AFTER).value(RETRY_AFTER_0))
                            .build();
                    doResponseBegin(traceId, authorization, beginEx);
                    doResponseEnd(traceId, authorization, EMPTY_OCTETS);

                    // count abandoned requests
                    countRequestsAbandoned.getAsLong();
                }
            }
        }

        private void onResponseReset(
            ResetFW reset)
        {
            final long traceId = reset.traceId();
            final long authorization = reset.authorization();

            state = HttpState.closeReply(state);
            client.cleanupNetwork(traceId, authorization);
        }

        private void onResponseWindow(
            WindowFW window)
        {
            final long sequence = window.sequence();
            final long acknowledge = window.acknowledge();
            final long traceId = window.traceId();
            final long authorization = window.authorization();
            final long budgetId = window.budgetId();
            final int maximum = window.maximum();
            final int padding = window.padding();

            assert acknowledge <= sequence;
            assert acknowledge >= replyAck;
            assert maximum >= replyMax;

            replyAck = acknowledge;
            replyMax = maximum;
            replyPad = padding;

            assert replyAck <= replySeq;

            state = HttpState.openReply(state);

            client.decodeNetworkIfBuffered(traceId, authorization);

            client.flushNetworkWindow(traceId, budgetId, replyPad);
        }

        private void onExchangeClosed()
        {
            assert client.exchange == this;
            client.exchange = null;
            client.pool.flushNext();
        }

        private void cleanup(
            long traceId,
            long authorization)
        {
            doRequestReset(traceId, authorization);
            doResponseAbort(traceId, authorization, EMPTY_OCTETS);
        }
    }

    private Map<String, String> asHeadersMap(
        Array32FW<HttpHeaderFW> headers)
    {
        Map<String, String> headersMap = new LinkedHashMap<>();
        headers.forEach(h -> headersMap.put(h.name().asString(), h.value().asString()));
        return headersMap;
    }

    private String8FW newString8FW(
        String8FW value)
    {
        return new String8FW().wrap(value.buffer(), value.offset(), value.limit());
    }

    private String16FW newString16FW(
        String16FW value)
    {
        return new String16FW().wrap(value.buffer(), value.offset(), value.limit());
    }
}
