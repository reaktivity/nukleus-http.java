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
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.http.internal.util.BufferUtil.indexOfByte;
import static org.reaktivity.nukleus.http.internal.util.BufferUtil.limitOfBytes;

import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.LongUnaryOperator;
import java.util.function.ToIntFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.agrona.AsciiSequenceView;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.MutableBoolean;
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
import org.reaktivity.nukleus.http.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http.internal.types.stream.HttpEndExFW;
import org.reaktivity.nukleus.http.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

public final class HttpServerFactory implements StreamFactory
{
    private static final Pattern REQUEST_LINE_PATTERN =
            Pattern.compile("(?<method>[A-Z]+)\\s+(?<target>[^\\s]+)\\s+(?<version>HTTP/\\d\\.\\d)\r\n");
    private static final Pattern VERSION_PATTERN = Pattern.compile("HTTP/1\\.\\d");
    private static final Pattern HEADER_LINE_PATTERN = Pattern.compile("(?<name>[^\\s:]+):\\s*(?<value>[^\r\n]*)\r\n");
    private static final Pattern CONNECTION_CLOSE_PATTERN = Pattern.compile("(^|\\s*,\\s*)close(\\s*,\\s*|$)");

    private static final byte[] COLON_SPACE_BYTES = ": ".getBytes(US_ASCII);
    private static final byte[] CRLFCRLF_BYTES = "\r\n\r\n".getBytes(US_ASCII);
    private static final byte[] CRLF_BYTES = "\r\n".getBytes(US_ASCII);
    private static final byte[] SEMICOLON_BYTES = ";".getBytes(US_ASCII);

    private static final byte COLON_BYTE = ':';
    private static final byte HYPHEN_BYTE = '-';
    private static final byte SPACE_BYTE = ' ';
    private static final byte ZERO_BYTE = '0';

    private static final byte[] HTTP_1_1_BYTES = "HTTP/1.1".getBytes(US_ASCII);
    private static final byte[] REASON_OK_BYTES = "OK".getBytes(US_ASCII);
    private static final byte[] REASON_SWITCHING_PROTOCOLS_BYTES = "Switching Protocols".getBytes(US_ASCII);

    private static final DirectBuffer ZERO_CHUNK = new UnsafeBuffer("0\r\n\r\n".getBytes(US_ASCII));

    private static final DirectBuffer ERROR_400_BAD_REQUEST =
            initResponse(400, "Bad Request");
    private static final DirectBuffer ERROR_400_BAD_REQUEST_OBSOLETE_LINE_FOLDING =
            initResponse(400, "Bad Request - obsolete line folding not supported");
    private static final DirectBuffer ERROR_404_NOT_FOUND =
            initResponse(404, "Not Found");
    private static final DirectBuffer ERROR_414_REQUEST_URI_TOO_LONG =
            initResponse(414, "Request URI Too Long");
    private static final DirectBuffer ERROR_431_HEADERS_TOO_LARGE =
            initResponse(431, "Request Header Fields Too Large");
    private static final DirectBuffer ERROR_501_UNSUPPORTED_TRANSFER_ENCODING =
            initResponse(501, "Unsupported Transfer-Encoding");
    private static final DirectBuffer ERROR_501_METHOD_NOT_IMPLEMENTED =
            initResponse(501, "Not Implemented");
    private static final DirectBuffer ERROR_505_VERSION_NOT_SUPPORTED =
            initResponse(505, "HTTP Version Not Supported");
    private static final DirectBuffer ERROR_507_INSUFFICIENT_STORAGE =
            initResponse(507, "Insufficient Storage");

    private static final String8FW HEADER_AUTHORITY = new String8FW(":authority");
    private static final String8FW HEADER_CONNECTION = new String8FW("connection");
    private static final String8FW HEADER_CONTENT_LENGTH = new String8FW("content-length");
    private static final String8FW HEADER_METHOD = new String8FW(":method");
    private static final String8FW HEADER_PATH = new String8FW(":path");
    private static final String8FW HEADER_SCHEME = new String8FW(":scheme");
    private static final String8FW HEADER_STATUS = new String8FW(":status");
    private static final String8FW HEADER_TRANSFER_ENCODING = new String8FW("transfer-encoding");
    private static final String8FW HEADER_UPGRADE = new String8FW("upgrade");

    private static final String16FW CONNECTION_CLOSE = new String16FW("close");
    private static final String16FW SCHEME_HTTP = new String16FW("http");
    private static final String16FW SCHEME_HTTPS = new String16FW("https");
    private static final String16FW STATUS_101 = new String16FW("101");
    private static final String16FW STATUS_200 = new String16FW("200");
    private static final String16FW TRANSFER_ENCODING_CHUNKED = new String16FW("chunked");

    private static final OctetsFW EMPTY_OCTETS = new OctetsFW().wrap(new UnsafeBuffer(new byte[0]), 0, 0);
    private static final Array32FW<HttpHeaderFW> DEFAULT_HEADERS =
            new Array32FW.Builder<>(new HttpHeaderFW.Builder(), new HttpHeaderFW())
                    .wrap(new UnsafeBuffer(new byte[64]), 0, 64)
                    .item(i -> i.name(HEADER_STATUS).value(STATUS_200))
                    .item(i -> i.name(HEADER_CONNECTION).value(CONNECTION_CLOSE))
                    .build();
    private static final Array32FW<HttpHeaderFW> DEFAULT_TRAILERS =
            new Array32FW.Builder<>(new HttpHeaderFW.Builder(), new HttpHeaderFW())
                         .wrap(new UnsafeBuffer(new byte[8]), 0, 8)
                         .build();

    private static final Map<String16FW, String> SCHEME_PORTS;

    private static final Set<String> SUPPORTED_METHODS =
            new HashSet<>(asList("GET",
                                 "HEAD",
                                 "POST",
                                 "PUT",
                                 "DELETE",
                                 "CONNECT",
                                 "OPTIONS",
                                 "TRACE"));

    private static final int MAXIMUM_METHOD_LENGTH = SUPPORTED_METHODS.stream().mapToInt(String::length).max().getAsInt();

    static
    {
        final Map<String16FW, String> schemePorts = new HashMap<>();
        schemePorts.put(SCHEME_HTTP, "80");
        schemePorts.put(SCHEME_HTTPS, "443");
        SCHEME_PORTS = schemePorts;
    }

    private final RouteFW routeRO = new RouteFW();
    private final HttpRouteExFW routeExRO = new HttpRouteExFW();

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final AbortFW abortRO = new AbortFW();

    private final HttpBeginExFW beginExRO = new HttpBeginExFW();
    private final HttpEndExFW endExRO = new HttpEndExFW();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private AbortFW.Builder abortRW = new AbortFW.Builder();

    private final HttpBeginExFW.Builder beginExRW = new HttpBeginExFW.Builder();
    private final HttpBeginExFW.Builder newBeginExRW = new HttpBeginExFW.Builder();
    private final HttpEndExFW.Builder endExRW = new HttpEndExFW.Builder();

    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();

    private final HttpServerDecoder decodeHeaders = this::decodeHeaders;
    private final HttpServerDecoder decodeHeadersOnly = this::decodeHeadersOnly;
    private final HttpServerDecoder decodeChunkHeader = this::decodeChunkHeader;
    private final HttpServerDecoder decodeChunkBody = this::decodeChunkBody;
    private final HttpServerDecoder decodeChunkEnd = this::decodeChunkEnd;
    private final HttpServerDecoder decodeContent = this::decodeContent;
    private final HttpServerDecoder decodeTrailers = this::decodeTrailers;
    private final HttpServerDecoder decodeEmptyLines = this::decodeEmptyLines;
    private final HttpServerDecoder decodeUpgraded = this::decodeUpgraded;
    private final HttpServerDecoder decodeIgnore = this::decodeIgnore;

    private final MessageFunction<RouteFW> wrapRoute = (t, b, i, l) -> routeRO.wrap(b, i, i + l);

    private final MutableInteger codecOffset = new MutableInteger();
    private final MutableBoolean hasAuthority = new MutableBoolean();

    private final RouteManager router;
    private final MutableDirectBuffer writeBuffer;
    private final MutableDirectBuffer codecBuffer;
    private final BufferPool bufferPool;
    private final LongUnaryOperator supplyInitialId;
    private final LongUnaryOperator supplyReplyId;
    private final int httpTypeId;
    private final Long2ObjectHashMap<HttpServer.HttpExchange> correlations;
    private final Matcher requestLine;
    private final Matcher versionPart;
    private final Matcher headerLine;
    private final Matcher connectionClose;
    private final int maximumHeadersSize;

    public HttpServerFactory(
        HttpConfiguration config,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        LongUnaryOperator supplyInitialId,
        LongUnaryOperator supplyReplyId,
        ToIntFunction<String> supplyTypeId)
    {
        this.router = requireNonNull(router);
        this.writeBuffer = requireNonNull(writeBuffer);
        this.codecBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
        this.bufferPool = requireNonNull(bufferPool);
        this.supplyInitialId = requireNonNull(supplyInitialId);
        this.supplyReplyId = requireNonNull(supplyReplyId);
        this.httpTypeId = supplyTypeId.applyAsInt(HttpNukleus.NAME);
        this.correlations = new Long2ObjectHashMap<>();
        this.requestLine = REQUEST_LINE_PATTERN.matcher("");
        this.headerLine = HEADER_LINE_PATTERN.matcher("");
        this.versionPart = VERSION_PATTERN.matcher("");
        this.connectionClose = CONNECTION_CLOSE_PATTERN.matcher("");
        this.maximumHeadersSize = bufferPool.slotCapacity();
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
            newStream = newNetworkStream(begin, sender);
        }
        else
        {
            newStream = newApplicationStream(begin, sender);
        }

        return newStream;
    }

    private MessageConsumer newNetworkStream(
        final BeginFW begin,
        final MessageConsumer network)
    {
        final long routeId = begin.routeId();
        final long authorization = begin.authorization();

        final MessagePredicate filter = (t, b, o, l) -> true;
        final RouteFW route = router.resolve(routeId, authorization, filter, wrapRoute);

        MessageConsumer newStream = null;

        if (route != null)
        {
            final long initialId = begin.streamId();
            final long affinity = begin.affinity();

            final HttpServer server = new HttpServer(network, routeId, initialId, affinity);
            newStream = server::onNetwork;
        }

        return newStream;
    }

    private MessageConsumer newApplicationStream(
        final BeginFW begin,
        final MessageConsumer application)
    {
        final long replyId = begin.streamId();

        MessageConsumer newStream = null;

        final HttpServer.HttpExchange exchange = correlations.remove(replyId);
        if (exchange != null)
        {
            newStream = exchange::onResponse;
        }

        return newStream;
    }

    private void doBegin(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        long authorization,
        long affinity,
        Flyweight extension)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                     .routeId(routeId)
                                     .streamId(streamId)
                                     .traceId(traceId)
                                     .authorization(authorization)
                                     .affinity(affinity)
                                     .extension(extension.buffer(), extension.offset(), extension.sizeof())
                                     .build();

        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    private void doData(
        MessageConsumer receiver,
        long routeId,
        long streamId,
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
        long traceId,
        long authorization,
        Flyweight extension)
    {
        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                               .routeId(routeId)
                               .streamId(streamId)
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
        long traceId,
        long authorization,
        Flyweight extension)
    {
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                     .routeId(routeId)
                                     .streamId(streamId)
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
        long traceId,
        long authorization)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                     .routeId(routeId)
                                     .streamId(streamId)
                                     .traceId(traceId)
                                     .authorization(authorization)
                                     .build();

        receiver.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }

    private void doWindow(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        long authorization,
        long budgetId,
        int credit,
        int padding)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                                        .routeId(routeId)
                                        .streamId(streamId)
                                        .traceId(traceId)
                                        .authorization(authorization)
                                        .budgetId(budgetId)
                                        .credit(credit)
                                        .padding(padding)
                                        .build();

        receiver.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    private int decodeHeaders(
        HttpServer server,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        final HttpBeginExFW.Builder httpBeginEx = beginExRW.wrap(codecBuffer, 0, codecBuffer.capacity())
                                                           .typeId(httpTypeId);

        DirectBuffer error = null;

        final int endOfStartAt = limitOfBytes(buffer, offset, limit, CRLF_BYTES);
        if (endOfStartAt != -1)
        {
            hasAuthority.value = false;
            error = decodeStartLine(buffer, offset, endOfStartAt, httpBeginEx, hasAuthority);
        }
        else if (limit - offset >= maximumHeadersSize)
        {
            error = ERROR_414_REQUEST_URI_TOO_LONG;
        }
        else
        {
            final int endOfMethodLimit = Math.min(limit, offset + MAXIMUM_METHOD_LENGTH);
            final int endOfMethodAt = indexOfByte(buffer, offset, endOfMethodLimit, SPACE_BYTE);
            if (endOfMethodAt != -1)
            {
                final CharSequence method = new AsciiSequenceView(buffer, offset, endOfMethodAt - offset);
                if (!SUPPORTED_METHODS.contains(method))
                {
                    error = ERROR_501_METHOD_NOT_IMPLEMENTED;
                }
            }
            else if (limit > endOfMethodLimit)
            {
                error = ERROR_400_BAD_REQUEST;
            }
        }

        final int endOfHeadersAt = limitOfBytes(buffer, offset, limit, CRLFCRLF_BYTES);
        if (error == null && endOfHeadersAt != -1)
        {
            server.decoder = decodeHeadersOnly;

            final int endOfHeaderLinesAt = endOfHeadersAt - CRLF_BYTES.length;
            int startOfLineAt = endOfStartAt;
            for (int endOfLineAt = limitOfBytes(buffer, startOfLineAt, endOfHeaderLinesAt, CRLF_BYTES);
                    endOfLineAt != -1 && error == null;
                    startOfLineAt = endOfLineAt,
                    endOfLineAt = limitOfBytes(buffer, startOfLineAt, endOfHeaderLinesAt, CRLF_BYTES))
            {
                error = decodeHeaderLine(server, buffer, offset, startOfLineAt, endOfLineAt,
                                         httpBeginEx, hasAuthority);
            }

            if (error == null && !hasAuthority.value)
            {
                error = ERROR_400_BAD_REQUEST;
            }

            if (error == null)
            {
                final HttpBeginExFW beginEx = httpBeginEx.build();
                final MessagePredicate routeable = (t, b, o, l) ->
                {
                    final RouteFW route = wrapRoute.apply(t, b, o, l);
                    final HttpRouteExFW routeEx = route.extension().get(routeExRO::tryWrap);
                    return routeEx == null || matchHeaders(routeEx.headers(), beginEx.headers());
                };

                final RouteFW route = router.resolve(server.routeId, authorization, routeable, wrapRoute);
                if (route != null)
                {
                    final long newRouteId = route.correlationId();
                    final HttpBeginExFW newBeginEx = updateHeaders(route, beginEx);
                    server.onDecodeHeaders(newRouteId, traceId, authorization, newBeginEx);
                }
                else
                {
                    error = ERROR_404_NOT_FOUND;
                }
            }
        }
        else if (error == null && limit - offset >= maximumHeadersSize)
        {
            error = ERROR_414_REQUEST_URI_TOO_LONG;
        }

        if (error != null)
        {
            server.onDecodeHeadersError(traceId, authorization, error);
            server.decoder = decodeIgnore;
        }

        return error == null && endOfHeadersAt != -1 ? endOfHeadersAt : offset;
    }

    private boolean matchHeaders(
        Array32FW<HttpHeaderFW> routeHeaders,
        Array32FW<HttpHeaderFW> beginHeaders)
    {
        final HttpHeaderFW schemeHeader = routeHeaders.matchFirst(h -> HEADER_SCHEME.equals(h.name()));
        final String16FW scheme = schemeHeader != null ? schemeHeader.value() : null;
        final String schemePort = SCHEME_PORTS.get(scheme);

        final BiPredicate<HttpHeaderFW, HttpHeaderFW> filter = (r, b) ->
                    r.name().equals(b.name()) &&
                    (HEADER_SCHEME.equals(r.name()) ||
                     HEADER_AUTHORITY.equals(r.name()) && matchAuthority(schemePort, r.value(), b.value()) ||
                     r.value().equals(b.value()));

        final MutableBoolean match = new MutableBoolean(true);
        routeHeaders.forEach(r -> match.value &= beginHeaders.anyMatch(b -> filter.test(r, b)));
        return match.value;
    }

    private boolean matchAuthority(
        String schemePort,
        String16FW canonicalAuthority,
        String16FW authority)
    {
        final DirectBuffer canonicalAuthorityValue = canonicalAuthority.value();
        final int canonicalPortAt = indexOfByte(canonicalAuthorityValue, 0, canonicalAuthorityValue.capacity(), COLON_BYTE);

        final DirectBuffer authorityValue = authority.value();
        final int portAt = indexOfByte(authorityValue, 0, authorityValue.capacity(), COLON_BYTE);

        final String routeAuthorityAsString = canonicalAuthority.asString();
        return canonicalAuthority.equals(authority) ||
                portAt == -1 && canonicalPortAt != -1 &&
                routeAuthorityAsString.regionMatches(0, authority.asString(), 0, canonicalPortAt) &&
                routeAuthorityAsString.regionMatches(canonicalPortAt + 1, schemePort, 0,
                                                     routeAuthorityAsString.length() - canonicalPortAt - 1);
    }

    private HttpBeginExFW updateHeaders(
        final RouteFW route,
        final HttpBeginExFW beginEx)
    {
        // update headers with the matched route's :scheme, :authority
        final HttpRouteExFW routeEx = route.extension().get(routeExRO::tryWrap);
        if (routeEx != null)
        {
            final HttpHeaderFW schemeHeader = routeEx.headers().matchFirst(h -> HEADER_SCHEME.equals(h.name()));
            final String scheme = schemeHeader != null ? schemeHeader.value().asString() : null;

            final HttpHeaderFW authorityHeader = routeEx.headers().matchFirst(h -> HEADER_AUTHORITY.equals(h.name()));
            final String authority = authorityHeader != null ? authorityHeader.value().asString() : null;

            if (scheme != null || authority != null || !routeEx.overrides().isEmpty())
            {
                final Map<String, String> headers = new LinkedHashMap<>();

                beginEx.headers().forEach(h ->
                {
                    String name = h.name().asString();
                    String value = h.value().asString();
                    headers.put(name, value);
                });

                if (scheme != null)
                {
                    headers.put(HEADER_SCHEME.asString(), scheme);
                }

                if (authority != null)
                {
                    headers.put(HEADER_AUTHORITY.asString(), authority);
                }

                routeEx.overrides().forEach(h ->
                {
                    String name = h.name().asString();
                    String value = h.value().asString();
                    headers.put(name, value);
                });

                final HttpBeginExFW.Builder newBeginEx = newBeginExRW.wrap(codecBuffer, 0, codecBuffer.capacity())
                                                                     .typeId(httpTypeId);

                headers.entrySet().forEach(e -> newBeginEx.headersItem(h -> h.name(e.getKey()).value(e.getValue())));

                return newBeginEx.build();
            }
        }

        return beginEx;
    }

    private DirectBuffer decodeStartLine(
        DirectBuffer buffer,
        int offset,
        int limit,
        HttpBeginExFW.Builder httpBeginEx,
        MutableBoolean hasAuthority)
    {
        DirectBuffer error = null;
        final CharSequence startLine = new AsciiSequenceView(buffer, offset, limit - offset);
        if (startLine.length() >= maximumHeadersSize)
        {
            error = ERROR_414_REQUEST_URI_TOO_LONG;
        }
        else if (requestLine.reset(startLine).matches())
        {
            final String method = requestLine.group("method");
            final String target = requestLine.group("target");
            final String version = requestLine.group("version");

            URI targetURI = null;
            try
            {
                targetURI = URI.create(target);
            }
            catch (IllegalArgumentException e)
            {
                //NOOP
            }

            if (targetURI == null)
            {
                error = ERROR_400_BAD_REQUEST;
            }
            else if (!versionPart.reset(version).matches())
            {
                error = ERROR_505_VERSION_NOT_SUPPORTED;
            }
            else if (targetURI.getUserInfo() != null)
            {
                error = ERROR_400_BAD_REQUEST;
            }
            else if (!SUPPORTED_METHODS.contains(method))
            {
                error = ERROR_501_METHOD_NOT_IMPLEMENTED;
            }
            else
            {
                final String path = targetURI.getRawPath();
                final String authority = targetURI.getAuthority();

                httpBeginEx.headersItem(h -> h.name(HEADER_SCHEME).value(SCHEME_HTTP))
                           .headersItem(h -> h.name(HEADER_METHOD).value(method))
                           .headersItem(h -> h.name(HEADER_PATH).value(path));

                if (authority != null)
                {
                    httpBeginEx.headersItem(h -> h.name(HEADER_AUTHORITY).value(authority));
                    hasAuthority.value = true;
                }
            }
        }
        else
        {
            error = ERROR_400_BAD_REQUEST;
        }

        return error;
    }

    private DirectBuffer decodeHeaderLine(
        HttpServer server,
        DirectBuffer buffer,
        int startOfHeadersAt,
        int startOfLineAt,
        int endOfLineAt,
        HttpBeginExFW.Builder httpBeginEx,
        MutableBoolean hasAuthority)
    {
        DirectBuffer error = null;

        if (endOfLineAt - startOfHeadersAt > maximumHeadersSize)
        {
            error = ERROR_431_HEADERS_TOO_LARGE;
        }
        else if (headerLine.reset(new AsciiSequenceView(buffer, startOfLineAt, endOfLineAt - startOfLineAt)).matches())
        {
            final String name = headerLine.group("name").toLowerCase();
            final String value = headerLine.group("value");

            switch (name)
            {
            case "content-length":
                if (server.decoder != decodeHeadersOnly)
                {
                    error = ERROR_400_BAD_REQUEST;
                }
                else
                {
                    final int contentLength = parseInt(value);
                    if (contentLength > 0)
                    {
                        server.decodableContentLength = contentLength;
                        server.decoder = decodeContent;
                    }
                    httpBeginEx.headersItem(h -> h.name(HEADER_CONTENT_LENGTH).value(value));
                }
                break;

            case "host":
                if (!hasAuthority.value)
                {
                    httpBeginEx.headersItem(h -> h.name(HEADER_AUTHORITY).value(value));
                    hasAuthority.value = true;
                }
                break;

            case "transfer-encoding":
                if (server.decoder != decodeHeadersOnly)
                {
                    error = ERROR_400_BAD_REQUEST;
                }
                else if (!"chunked".equals(value))
                {
                    error = ERROR_501_UNSUPPORTED_TRANSFER_ENCODING;
                }
                else
                {
                    server.decoder = decodeChunkHeader;
                    httpBeginEx.headersItem(h -> h.name(HEADER_TRANSFER_ENCODING).value(TRANSFER_ENCODING_CHUNKED));
                }
                break;

            case "upgrade":
                if (server.decoder != decodeHeadersOnly)
                {
                    error = ERROR_400_BAD_REQUEST;
                }
                else
                {
                    // TODO: wait for 101 first
                    server.decoder = decodeUpgraded;
                    httpBeginEx.headersItem(h -> h.name(HEADER_UPGRADE).value(value));
                }
                break;

            default:
                httpBeginEx.headersItem(h -> h.name(name).value(value));
                break;
            }
        }
        else if (buffer.getByte(startOfLineAt) == SPACE_BYTE)
        {
            error = ERROR_400_BAD_REQUEST_OBSOLETE_LINE_FOLDING;
        }
        else
        {
            error = ERROR_400_BAD_REQUEST;
        }

        return error;
    }

    private int decodeHeadersOnly(
        HttpServer server,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        server.onDecodeHeadersOnly(traceId, authorization, EMPTY_OCTETS);
        server.decoder = decodeEmptyLines;
        return offset;
    }

    private int decodeChunkHeader(
        HttpServer server,
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
                server.decodableChunkSize = Integer.parseInt(chunkSizeHex, 0, chunkSizeLength, 16);
                server.decoder = server.decodableChunkSize != 0 ? decodeChunkBody : decodeTrailers;
                progress = chunkHeaderLimit;
            }
            catch (NumberFormatException ex)
            {
                server.onDecodeHeadersError(traceId, authorization, ERROR_400_BAD_REQUEST);
                server.decoder = decodeIgnore;
            }
        }

        return progress;
    }

    private int decodeChunkBody(
        HttpServer server,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        final int decodableBytes = Math.min(limit - offset, server.decodableChunkSize);

        int progress = offset;
        if (decodableBytes > 0)
        {
            progress = server.onDecodeBody(traceId, authorization, budgetId,
                                           buffer, offset, offset + decodableBytes, EMPTY_OCTETS);
            server.decodableChunkSize -= progress - offset;

            if (server.decodableChunkSize == 0)
            {
                server.decoder = decodeChunkEnd;
            }
        }

        return progress;
    }

    private int decodeChunkEnd(
        HttpServer server,
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
                server.onDecodeBodyError(traceId, authorization, ERROR_400_BAD_REQUEST);
                server.decoder = decodeIgnore;
            }
            else
            {
                server.decoder = decodeChunkHeader;
                progress += 2;
            }
        }
        return progress;
    }

    private int decodeContent(
        HttpServer server,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        final int length = Math.min(limit - offset, server.decodableContentLength);

        int progress = offset;
        if (length > 0)
        {
            progress = server.onDecodeBody(traceId, authorization, budgetId, buffer, offset, offset + length, EMPTY_OCTETS);
            server.decodableContentLength -= progress - offset;
        }

        assert server.decodableContentLength >= 0;

        if (server.decodableContentLength == 0)
        {
            server.onDecodeTrailers(traceId, authorization, EMPTY_OCTETS);
            server.decoder = decodeEmptyLines;
        }

        return progress;
    }

    private int decodeTrailers(
        HttpServer server,
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

            server.onDecodeTrailers(traceId, authorization, httpEndEx);
            progress = endOfTrailersAt;
            server.decoder = decodeEmptyLines;
        }
        else if (buffer.getByte(offset) == '\r' &&
            buffer.getByte(offset + 1) == '\n')
        {
            server.onDecodeTrailers(traceId, authorization, EMPTY_OCTETS);
            progress += 2;
            server.decoder = decodeEmptyLines;
        }

        return progress;
    }

    private int decodeEmptyLines(
        HttpServer server,
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
                server.decoder = decodeHeaders;
            }
        }
        return progress;
    }

    private int decodeUpgraded(
        HttpServer server,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        return server.onDecodeBody(traceId, authorization, budgetId, buffer, offset, limit, EMPTY_OCTETS);
    }

    private int decodeIgnore(
        HttpServer server,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        server.doNetworkWindow(traceId, authorization, budgetId, reserved, 0);
        return limit;
    }

    @FunctionalInterface
    private interface HttpServerDecoder
    {
        int decode(
            HttpServer server,
            long traceId,
            long authorization,
            long budgetId,
            int reserved,
            DirectBuffer buffer,
            int offset,
            int limit);
    }

    private enum HttpState
    {
        PENDING,
        OPEN,
        CLOSED,
    }

    private final class HttpServer
    {
        private final MessageConsumer network;
        private final long routeId;
        private final long initialId;
        private final long replyId;
        private final long affinity;

        private int initialBudget;
        private int replyPadding;
        private int replyBudget;
        private boolean replyCloseOnFlush;

        private int decodeSlot;
        private int decodeSlotOffset;
        private int decodeSlotReserved;

        private int encodeSlot;
        private int encodeSlotOffset;

        private HttpServerDecoder decoder;
        private int decodableChunkSize;
        private int decodableContentLength;

        private HttpExchange exchange;

        private HttpServer(
            MessageConsumer network,
            long routeId,
            long initialId,
            long affinity)
        {
            this.network = network;
            this.routeId = routeId;
            this.initialId = initialId;
            this.affinity = affinity;
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.decoder = decodeEmptyLines;
            this.decodeSlot = NO_SLOT;
            this.encodeSlot = NO_SLOT;
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

            doNetworkWindow(traceId, authorization, 0, bufferPool.slotCapacity(), 0);
            doNetworkBegin(traceId, authorization, affinity);
        }

        private void onNetworkData(
            DataFW data)
        {
            final long traceId = data.traceId();
            final long authorization = data.authorization();
            final long budgetId = data.budgetId();

            initialBudget -= data.reserved();

            if (initialBudget < 0)
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
            if (decodeSlot == NO_SLOT)
            {
                final long traceId = end.traceId();
                final long authorization = end.authorization();

                cleanupDecodeSlotIfNecessary();

                if (exchange != null)
                {
                    exchange.onNetworkEnd(traceId, authorization);
                }
                else
                {
                    doNetworkEnd(traceId, authorization);
                }
            }

            replyCloseOnFlush = true;
        }

        private void onNetworkAbort(
            AbortFW abort)
        {
            final long traceId = abort.traceId();
            final long authorization = abort.authorization();

            cleanupDecodeSlotIfNecessary();

            if (exchange != null)
            {
                exchange.onNetworkAbort(traceId, authorization);
                exchange.onNetworkReset(traceId, authorization);
                doNetworkAbort(traceId, authorization);
            }
            else
            {
                doNetworkEnd(traceId, authorization);
            }
        }

        private void onNetworkReset(
            ResetFW reset)
        {
            final long traceId = reset.traceId();
            final long authorization = reset.authorization();

            cleanupEncodeSlotIfNecessary();

            if (exchange != null)
            {
                exchange.onNetworkReset(traceId, authorization);
            }
            else
            {
                doNetworkReset(traceId, authorization);
            }
        }

        private void onNetworkWindow(
            WindowFW window)
        {
            final long traceId = window.traceId();
            final long authorization = window.authorization();
            final long budgetId = window.budgetId();
            final int credit = window.credit();
            final int padding = window.padding();

            replyBudget += credit;
            replyPadding = padding;

            flushNetworkIfBuffered(traceId, authorization, budgetId);

            if (exchange != null && exchange.responseState == HttpState.OPEN && replyBudget > replyPadding)
            {
                exchange.doResponseWindow(traceId, authorization, budgetId, replyBudget, replyPadding);
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
                final int reserved = limit + replyPadding;
                doNetworkData(traceId, authorization, budgetId, reserved, buffer, 0, limit);
            }
        }

        private void doNetworkBegin(
            long traceId,
            long authorization,
            long affinity)
        {
            doBegin(network, routeId, replyId, traceId, authorization, affinity, EMPTY_OCTETS);
            router.setThrottle(replyId, this::onNetwork);
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
            final int length = Math.min(replyBudget - replyPadding, maxLength);

            if (length > 0)
            {
                final int required = length + replyPadding;

                assert reserved >= required;

                replyBudget -= required;

                assert replyBudget >= 0;

                doData(network, routeId, replyId, traceId, authorization, budgetId,
                       required, buffer, offset, length, EMPTY_OCTETS);
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

                if (exchange == null && replyCloseOnFlush)
                {
                    doNetworkEnd(traceId, authorization);
                }
            }
        }

        private void doNetworkEnd(
            long traceId,
            long authorization)
        {
            cleanupEncodeSlotIfNecessary();
            doEnd(network, routeId, replyId, traceId, authorization, EMPTY_OCTETS);
        }

        private void doNetworkAbort(
            long traceId,
            long authorization)
        {
            cleanupEncodeSlotIfNecessary();
            doAbort(network, routeId, replyId, traceId, authorization, EMPTY_OCTETS);
        }

        private void doNetworkReset(
            long traceId,
            long authorization)
        {
            cleanupDecodeSlotIfNecessary();
            doReset(network, routeId, initialId, traceId, authorization);
        }

        private void doNetworkWindow(
            long traceId,
            long authorization,
            long budgetId,
            int credit,
            int padding)
        {
            assert credit > 0;

            initialBudget += credit;
            doWindow(network, routeId, initialId, traceId, authorization, budgetId, credit, padding);
        }

        private void decodeNetworkIfBuffered(
            long traceId,
            long authorization,
            long budgetId)
        {
            if (decodeSlot != NO_SLOT)
            {
                final MutableDirectBuffer decodeBuffer = bufferPool.buffer(decodeSlot);
                final int decodeLength = decodeSlotOffset;
                decodeNetwork(traceId, authorization, budgetId, decodeSlotReserved, decodeBuffer, 0, decodeLength);
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
            HttpServerDecoder previous = null;
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
            }
        }

        private void onDecodeHeadersError(
            long traceId,
            long authorization,
            DirectBuffer error)
        {
            doNetworkData(traceId, authorization, 0L, error.capacity() + replyPadding, error, 0, error.capacity());
            doNetworkEnd(traceId, authorization);

            assert exchange == null;
        }

        private void onDecodeBodyError(
            long traceId,
            long authorization,
            DirectBuffer error)
        {
            cleanupNetwork(traceId, authorization);
        }

        private void onDecodeHeaders(
            long routeId,
            long traceId,
            long authorization,
            HttpBeginExFW beginEx)
        {
            final long initialId = supplyInitialId.applyAsLong(routeId);
            final long replyId = supplyReplyId.applyAsLong(initialId);
            final MessageConsumer application = router.supplyReceiver(initialId);

            final HttpExchange exchange = new HttpExchange(application, routeId, initialId, replyId);
            exchange.doRequestBegin(traceId, authorization, beginEx);
            correlations.put(replyId, exchange);

            final HttpHeaderFW connection = beginEx.headers().matchFirst(h -> HEADER_CONNECTION.equals(h.name()));
            exchange.responseClosing = connection != null && connectionClose.reset(connection.value().asString()).matches();

            this.exchange = exchange;
        }

        private void onDecodeHeadersOnly(
            long traceId,
            long authorization,
            Flyweight extension)
        {
            exchange.doRequestEnd(traceId, authorization, extension);
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
            return exchange.doRequestData(traceId, authorization, budgetId, buffer, offset, limit, extension);
        }

        private void onDecodeTrailers(
            long traceId,
            long authorization,
            Flyweight extension)
        {
            exchange.doRequestEnd(traceId, authorization, extension);

            if (exchange.requestState == HttpState.CLOSED &&
                exchange.responseState == HttpState.CLOSED)
            {
                exchange = null;
            }
        }

        private void doEncodeHeaders(
            HttpExchange exchange,
            long traceId,
            long authorization,
            long budgetId,
            Array32FW<HttpHeaderFW> headers)
        {
            // TODO: queue if pipelined responses arrive out of order
            assert exchange == this.exchange;

            final HttpHeaderFW transferEncoding = headers.matchFirst(h -> HEADER_TRANSFER_ENCODING.equals(h.name()));
            exchange.responseChunked = transferEncoding != null && TRANSFER_ENCODING_CHUNKED.equals(transferEncoding.value());

            final HttpHeaderFW connection = headers.matchFirst(h -> HEADER_CONNECTION.equals(h.name()));
            exchange.responseClosing |= connection != null && connectionClose.reset(connection.value().asString()).matches();

            final HttpHeaderFW upgrade = headers.matchFirst(h -> HEADER_UPGRADE.equals(h.name()));
            exchange.responseClosing |= upgrade != null;

            final HttpHeaderFW status = headers.matchFirst(h -> HEADER_STATUS.equals(h.name()));
            final String16FW statusValue = status != null ? status.value() : STATUS_200;
            codecOffset.value = doEncodeStatus(codecBuffer, 0, statusValue);
            headers.forEach(h -> codecOffset.value = doEncodeHeader(codecBuffer, codecOffset.value, h));
            codecBuffer.putBytes(codecOffset.value, CRLF_BYTES);
            codecOffset.value += CRLF_BYTES.length;

            final int length = codecOffset.value;

            if (length > maximumHeadersSize)
            {
                exchange.onNetworkReset(traceId, authorization);

                replyCloseOnFlush = true;

                DirectBuffer error = ERROR_507_INSUFFICIENT_STORAGE;
                doNetworkData(traceId, authorization, 0L, error.capacity() + replyPadding, error, 0, error.capacity());
            }
            else
            {
                final int reserved = length + replyPadding;
                doNetworkData(traceId, authorization, budgetId, reserved, codecBuffer, 0, length);
            }
        }

        private int doEncodeStatus(
            MutableDirectBuffer buffer,
            int offset,
            String16FW status)
        {
            int progress = offset;

            buffer.putBytes(progress, HTTP_1_1_BYTES);
            progress += HTTP_1_1_BYTES.length;

            buffer.putByte(progress, SPACE_BYTE);
            progress++;

            final DirectBuffer value = status.value();
            buffer.putBytes(progress, value, 0, value.capacity());
            progress += value.capacity();

            buffer.putByte(progress, SPACE_BYTE);
            progress++;

            byte[] reason = STATUS_101.equals(status) ? REASON_SWITCHING_PROTOCOLS_BYTES : REASON_OK_BYTES;
            buffer.putBytes(progress, reason);
            progress += reason.length;

            buffer.putBytes(progress, CRLF_BYTES);
            progress += CRLF_BYTES.length;

            return progress;
        }

        private int doEncodeHeader(
            MutableDirectBuffer buffer,
            int offset,
            HttpHeaderFW header)
        {
            int progress = offset;
            final DirectBuffer name = header.name().value();
            if (name.getByte(0) != COLON_BYTE)
            {
                final DirectBuffer value = header.value().value();

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

            if (exchange.responseChunked && flags != 0)
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

            if (exchange.responseChunked)
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
                    trailers.forEach(h -> codecOffset.value = doEncodeHeader(writeBuffer, codecOffset.value, h));
                    codecBuffer.putBytes(codecOffset.value, CRLF_BYTES);
                    codecOffset.value += CRLF_BYTES.length;

                    buffer = codecBuffer;
                    offset = 0;
                    limit = codecOffset.value;
                }

                final int reserved = limit + replyPadding;
                doNetworkData(traceId, authorization, budgetId, reserved, buffer, offset, limit);
            }

            if (replyCloseOnFlush || exchange.responseClosing)
            {
                doNetworkEnd(traceId, authorization);
            }

            if (exchange.requestState == HttpState.CLOSED &&
                exchange.responseState == HttpState.CLOSED)
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
                exchange.onNetworkAbort(traceId, authorization);
                exchange.onNetworkReset(traceId, authorization);
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

        private final class HttpExchange
        {
            private final MessageConsumer application;
            private final long routeId;
            private final long requestId;
            private final long responseId;

            private int requestBudget;
            private int requestPadding;
            private int responseBudget;

            private HttpState requestState;
            private HttpState responseState;
            private boolean responseChunked;
            private boolean responseClosing;

            private HttpExchange(
                MessageConsumer application,
                long routeId,
                long requestId,
                long responseId)
            {
                this.application = application;
                this.routeId = routeId;
                this.requestId = requestId;
                this.responseId = responseId;
                this.requestState = HttpState.PENDING;
                this.responseState = HttpState.PENDING;
            }

            private void doRequestBegin(
                long traceId,
                long authorization,
                Flyweight extension)
            {
                doBegin(application, routeId, requestId, traceId, authorization, affinity, extension);
                router.setThrottle(requestId, this::onRequest);
            }

            private int doRequestData(
                long traceId,
                long authorization,
                long budgetId,
                DirectBuffer buffer,
                int offset,
                int limit,
                Flyweight extension)
            {
                int length = Math.min(requestBudget - requestPadding, limit - offset);

                if (length > 0)
                {
                    final int reserved = length + requestPadding;

                    requestBudget -= reserved;

                    assert requestBudget >= 0;

                    doData(application, routeId, requestId, traceId, authorization, budgetId,
                           reserved, buffer, offset, length, extension);
                }

                return offset + length;
            }

            private void doRequestEnd(
                long traceId,
                long authorization,
                Flyweight extension)
            {
                switch (requestState)
                {
                case OPEN:
                    doEnd(application, routeId, requestId, traceId, authorization, extension);
                    break;
                default:
                    requestState = HttpState.CLOSED;
                    break;
                }
            }

            private void doRequestAbort(
                long traceId,
                long authorization,
                Flyweight extension)
            {
                doAbort(application, routeId, requestId, traceId, authorization, extension);
                requestState = HttpState.CLOSED;
            }

            private void onNetworkEnd(
                long traceId,
                long authorization)
            {
                if (requestState != HttpState.CLOSED)
                {
                    doRequestAbort(traceId, authorization, EMPTY_OCTETS);
                }
            }

            private void onNetworkAbort(
                long traceId,
                long authorization)
            {
                if (requestState != HttpState.CLOSED)
                {
                    doRequestAbort(traceId, authorization, EMPTY_OCTETS);
                }
            }

            private void onNetworkReset(
                long traceId,
                long authorization)
            {
                correlations.remove(responseId);

                if (responseState == HttpState.OPEN)
                {
                    doResponseReset(traceId, authorization);
                }
            }

            private void onRequest(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
            {
                switch (msgTypeId)
                {
                case ResetFW.TYPE_ID:
                    final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                    onRequestReset(reset);
                    break;
                case WindowFW.TYPE_ID:
                    final WindowFW window = windowRO.wrap(buffer, index, index + length);
                    onRequestWindow(window);
                    break;
                }
            }

            private void onRequestReset(
                ResetFW reset)
            {
                final long traceId = reset.traceId();
                final long authorization = reset.authorization();
                requestState = HttpState.CLOSED;
                doNetworkReset(traceId, authorization);
            }

            private void onRequestWindow(
                WindowFW window)
            {
                final long traceId = window.traceId();
                final long authorization = window.authorization();
                final long budgetId = window.budgetId();
                final int credit = window.credit();
                final int padding = window.padding();

                if (requestState == HttpState.PENDING)
                {
                    requestState = HttpState.OPEN;
                }

                requestBudget += credit;
                requestPadding = padding;

                decodeNetworkIfBuffered(traceId, authorization, budgetId);

                if (decodeSlot == NO_SLOT && requestState == HttpState.CLOSED)
                {
                    // TODO: non-empty extension?
                    doEnd(application, routeId, requestId, traceId, authorization, EMPTY_OCTETS);
                }
                else
                {
                    final int initialCredit = Math.max(requestBudget - initialBudget, 0);
                    if (initialCredit > 0)
                    {
                        doNetworkWindow(traceId, authorization, budgetId, initialCredit, padding);
                    }
                }
            }

            private void onResponse(
                int msgTypeId,
                DirectBuffer buffer,
                int index,
                int length)
            {
                switch (msgTypeId)
                {
                case BeginFW.TYPE_ID:
                    final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                    onResponseBegin(begin);
                    break;
                case DataFW.TYPE_ID:
                    final DataFW data = dataRO.wrap(buffer, index, index + length);
                    onResponseData(data);
                    break;
                case EndFW.TYPE_ID:
                    final EndFW end = endRO.wrap(buffer, index, index + length);
                    onResponseEnd(end);
                    break;
                case AbortFW.TYPE_ID:
                    final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                    onResponseAbort(abort);
                    break;
                }
            }

            private void onResponseBegin(
                BeginFW begin)
            {
                final HttpBeginExFW beginEx = begin.extension().get(beginExRO::tryWrap);
                final Array32FW<HttpHeaderFW> headers = beginEx != null ? beginEx.headers() : DEFAULT_HEADERS;

                final long traceId = begin.traceId();
                final long authorization = begin.authorization();

                responseState = HttpState.OPEN;
                doEncodeHeaders(this, traceId, authorization, 0L, headers);
            }

            private void onResponseData(
                DataFW data)
            {
                responseBudget -= data.reserved();

                if (responseBudget < 0)
                {
                    final long traceId = data.traceId();
                    final long authorization = data.authorization();
                    doResponseReset(traceId, authorization);
                    doNetworkAbort(traceId, authorization);
                }
                else
                {
                    final long traceId = data.traceId();
                    final long authorization = data.authorization();
                    final int flags = data.flags();
                    final long budgetId = data.budgetId();
                    final int reserved = data.reserved();
                    final OctetsFW payload = data.payload();

                    doEncodeBody(this, traceId, authorization, flags, budgetId, reserved, payload);
                }
            }

            private void onResponseEnd(
                EndFW end)
            {
                final HttpEndExFW endEx = end.extension().get(endExRO::tryWrap);
                final Array32FW<HttpHeaderFW> trailers = endEx != null ? endEx.trailers() : DEFAULT_TRAILERS;

                final long traceId = end.traceId();
                final long authorization = end.authorization();

                responseState = HttpState.CLOSED;
                doEncodeTrailers(this, traceId, authorization, 0L, trailers);
            }

            private void onResponseAbort(
                AbortFW abort)
            {
                final long traceId = abort.traceId();
                final long authorization = abort.authorization();

                responseState = HttpState.CLOSED;
                doNetworkAbort(traceId, authorization);
            }

            private void doResponseReset(
                long traceId,
                long authorization)
            {
                responseState = HttpState.CLOSED;
                doReset(application, routeId, responseId, traceId, authorization);
            }

            private void doResponseWindow(
                long traceId,
                long authorization,
                long budgetId,
                int replyBudget,
                int replyPadding)
            {
                final int credit = Math.max(replyBudget - responseBudget, 0);

                if (credit > 0)
                {
                    responseBudget += credit;
                    doWindow(application, routeId, responseId, traceId, authorization, budgetId, credit, replyPadding);
                }
            }
        }
    }

    private static DirectBuffer initResponse(
        int status,
        String reason)
    {
        return new UnsafeBuffer(String.format("HTTP/1.1 %d %s\r\n" +
                                              "Connection: close\r\n" +
                                              "\r\n",
                                              status, reason).getBytes(UTF_8));
    }
}
