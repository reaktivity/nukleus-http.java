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
package org.reaktivity.nukleus.http2.internal.stream;

import static java.util.Objects.requireNonNull;
import static org.reaktivity.nukleus.budget.BudgetCreditor.NO_CREDITOR_INDEX;
import static org.reaktivity.nukleus.budget.BudgetDebitor.NO_DEBITOR_INDEX;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.http2.internal.hpack.HpackContext.CONNECTION;
import static org.reaktivity.nukleus.http2.internal.hpack.HpackContext.DEFAULT_ACCESS_CONTROL_ALLOW_ORIGIN;
import static org.reaktivity.nukleus.http2.internal.hpack.HpackContext.KEEP_ALIVE;
import static org.reaktivity.nukleus.http2.internal.hpack.HpackContext.PROXY_CONNECTION;
import static org.reaktivity.nukleus.http2.internal.hpack.HpackContext.TE;
import static org.reaktivity.nukleus.http2.internal.hpack.HpackContext.TRAILERS;
import static org.reaktivity.nukleus.http2.internal.hpack.HpackContext.UPGRADE;
import static org.reaktivity.nukleus.http2.internal.hpack.HpackHeaderFieldFW.HeaderFieldType.UNKNOWN;
import static org.reaktivity.nukleus.http2.internal.hpack.HpackLiteralHeaderFieldFW.LiteralType.INCREMENTAL_INDEXING;
import static org.reaktivity.nukleus.http2.internal.hpack.HpackLiteralHeaderFieldFW.LiteralType.WITHOUT_INDEXING;

import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongHashSet;
import org.agrona.collections.LongLongConsumer;
import org.agrona.collections.MutableBoolean;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.budget.BudgetCreditor;
import org.reaktivity.nukleus.budget.BudgetDebitor;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.concurrent.Signaler;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
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
import org.reaktivity.nukleus.http.internal.types.stream.HttpDataExFW;
import org.reaktivity.nukleus.http.internal.types.stream.HttpEndExFW;
import org.reaktivity.nukleus.http.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.http.internal.util.HttpUtil;
import org.reaktivity.nukleus.http2.internal.Http2Configuration;
import org.reaktivity.nukleus.http2.internal.Http2Counters;
import org.reaktivity.nukleus.http2.internal.hpack.HpackContext;
import org.reaktivity.nukleus.http2.internal.hpack.HpackHeaderBlockFW;
import org.reaktivity.nukleus.http2.internal.hpack.HpackHeaderFieldFW;
import org.reaktivity.nukleus.http2.internal.hpack.HpackHuffman;
import org.reaktivity.nukleus.http2.internal.hpack.HpackLiteralHeaderFieldFW;
import org.reaktivity.nukleus.http2.internal.hpack.HpackStringFW;
import org.reaktivity.nukleus.http2.internal.types.Http2ContinuationFW;
import org.reaktivity.nukleus.http2.internal.types.Http2DataFW;
import org.reaktivity.nukleus.http2.internal.types.Http2ErrorCode;
import org.reaktivity.nukleus.http2.internal.types.Http2FrameInfoFW;
import org.reaktivity.nukleus.http2.internal.types.Http2FrameType;
import org.reaktivity.nukleus.http2.internal.types.Http2GoawayFW;
import org.reaktivity.nukleus.http2.internal.types.Http2HeadersFW;
import org.reaktivity.nukleus.http2.internal.types.Http2PingFW;
import org.reaktivity.nukleus.http2.internal.types.Http2PrefaceFW;
import org.reaktivity.nukleus.http2.internal.types.Http2PriorityFW;
import org.reaktivity.nukleus.http2.internal.types.Http2PushPromiseFW;
import org.reaktivity.nukleus.http2.internal.types.Http2RstStreamFW;
import org.reaktivity.nukleus.http2.internal.types.Http2Setting;
import org.reaktivity.nukleus.http2.internal.types.Http2SettingsFW;
import org.reaktivity.nukleus.http2.internal.types.Http2WindowUpdateFW;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

public final class Http2ServerFactory implements StreamFactory
{
    private static final int CLIENT_INITIATED = 1;
    private static final int SERVER_INITIATED = 0;

    private static final int CLEANUP_SIGNAL = 0;

    private static final long MAX_REMOTE_BUDGET = Integer.MAX_VALUE;

    private static final DirectBuffer EMPTY_BUFFER = new UnsafeBuffer(new byte[0]);
    private static final OctetsFW EMPTY_OCTETS = new OctetsFW().wrap(EMPTY_BUFFER, 0, 0);

    private static final String8FW HEADER_PATH = new String8FW(":path");

    private static final Array32FW<HttpHeaderFW> HEADERS_200_OK =
        new Array32FW.Builder<>(new HttpHeaderFW.Builder(), new HttpHeaderFW())
            .wrap(new UnsafeBuffer(new byte[64]), 0, 64)
            .item(h -> h.name(":status").value("200"))
            .build();

    private static final Array32FW<HttpHeaderFW> HEADERS_404_NOT_FOUND =
        new Array32FW.Builder<>(new HttpHeaderFW.Builder(), new HttpHeaderFW())
            .wrap(new UnsafeBuffer(new byte[64]), 0, 64)
            .item(h -> h.name(":status").value("404"))
            .build();

    private static final Array32FW<HttpHeaderFW> HEADERS_400_BAD_REQUEST =
        new Array32FW.Builder<>(new HttpHeaderFW.Builder(), new HttpHeaderFW())
            .wrap(new UnsafeBuffer(new byte[64]), 0, 64)
            .item(h -> h.name(":status").value("400"))
            .build();

    private static final Array32FW<HttpHeaderFW> TRAILERS_EMPTY =
        new Array32FW.Builder<>(new HttpHeaderFW.Builder(), new HttpHeaderFW())
            .wrap(new UnsafeBuffer(new byte[64]), 0, 64)
            .build();

    private final RouteFW routeRO = new RouteFW();
    private final HttpRouteExFW routeExRO = new HttpRouteExFW();

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final AbortFW abortRO = new AbortFW();

    private final HttpBeginExFW beginExRO = new HttpBeginExFW();
    private final HttpDataExFW dataExRO = new HttpDataExFW();
    private final HttpEndExFW endExRO = new HttpEndExFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();

    private final HttpBeginExFW.Builder beginExRW = new HttpBeginExFW.Builder();
    private final HttpEndExFW.Builder endExRW = new HttpEndExFW.Builder();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();

    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();

    private final Http2PrefaceFW http2PrefaceRO = new Http2PrefaceFW();
    private final Http2FrameInfoFW http2FrameInfoRO = new Http2FrameInfoFW();
    private final Http2SettingsFW http2SettingsRO = new Http2SettingsFW();
    private final Http2GoawayFW http2GoawayRO = new Http2GoawayFW();
    private final Http2PingFW http2PingRO = new Http2PingFW();
    private final Http2DataFW http2DataRO = new Http2DataFW();
    private final Http2HeadersFW http2HeadersRO = new Http2HeadersFW();
    private final Http2ContinuationFW http2ContinuationRO = new Http2ContinuationFW();
    private final Http2WindowUpdateFW http2WindowUpdateRO = new Http2WindowUpdateFW();
    private final Http2RstStreamFW http2RstStreamRO = new Http2RstStreamFW();
    private final Http2PriorityFW http2PriorityRO = new Http2PriorityFW();

    private final Http2SettingsFW.Builder http2SettingsRW = new Http2SettingsFW.Builder();
    private final Http2GoawayFW.Builder http2GoawayRW = new Http2GoawayFW.Builder();
    private final Http2PingFW.Builder http2PingRW = new Http2PingFW.Builder();
    private final Http2DataFW.Builder http2DataRW = new Http2DataFW.Builder();
    private final Http2HeadersFW.Builder http2HeadersRW = new Http2HeadersFW.Builder();
    private final Http2WindowUpdateFW.Builder http2WindowUpdateRW = new Http2WindowUpdateFW.Builder();
    private final Http2RstStreamFW.Builder http2RstStreamRW = new Http2RstStreamFW.Builder();
    private final Http2PushPromiseFW.Builder http2PushPromiseRW = new Http2PushPromiseFW.Builder();

    private final HpackHeaderBlockFW headerBlockRO = new HpackHeaderBlockFW();

    private final Http2ServerDecoder decodePreface = this::decodePreface;
    private final Http2ServerDecoder decodeFrameType = this::decodeFrameType;
    private final Http2ServerDecoder decodeSettings = this::decodeSettings;
    private final Http2ServerDecoder decodePing = this::decodePing;
    private final Http2ServerDecoder decodeGoaway = this::decodeGoaway;
    private final Http2ServerDecoder decodeWindowUpdate = this::decodeWindowUpdate;
    private final Http2ServerDecoder decodeHeaders = this::decodeHeaders;
    private final Http2ServerDecoder decodeContinuation = this::decodeContinuation;
    private final Http2ServerDecoder decodeData = this::decodeData;
    private final Http2ServerDecoder decodePriority = this::decodePriority;
    private final Http2ServerDecoder decodeRstStream = this::decodeRstStream;
    private final Http2ServerDecoder decodeIgnoreOne = this::decodeIgnoreOne;
    private final Http2ServerDecoder decodeIgnoreAll = this::decodeIgnoreAll;

    private final EnumMap<Http2FrameType, Http2ServerDecoder> decodersByFrameType;
    {
        final EnumMap<Http2FrameType, Http2ServerDecoder> decodersByFrameType = new EnumMap<>(Http2FrameType.class);
        decodersByFrameType.put(Http2FrameType.SETTINGS, decodeSettings);
        decodersByFrameType.put(Http2FrameType.PING, decodePing);
        decodersByFrameType.put(Http2FrameType.GO_AWAY, decodeGoaway);
        decodersByFrameType.put(Http2FrameType.WINDOW_UPDATE, decodeWindowUpdate);
        decodersByFrameType.put(Http2FrameType.HEADERS, decodeHeaders);
        decodersByFrameType.put(Http2FrameType.CONTINUATION, decodeContinuation);
        decodersByFrameType.put(Http2FrameType.DATA, decodeData);
        decodersByFrameType.put(Http2FrameType.PRIORITY, decodePriority);
        decodersByFrameType.put(Http2FrameType.RST_STREAM, decodeRstStream);
        this.decodersByFrameType = decodersByFrameType;
    }

    private final MessageFunction<RouteFW> wrapRoute = (t, b, i, l) -> routeRO.wrap(b, i, i + l);

    private final Http2HeadersDecoder headersDecoder = new Http2HeadersDecoder();
    private final Http2HeadersEncoder headersEncoder = new Http2HeadersEncoder();

    private final Http2Configuration config;
    private final RouteManager router;
    private final MutableDirectBuffer writeBuffer;
    private final MutableDirectBuffer frameBuffer;
    private final BufferPool bufferPool;
    private final BudgetCreditor creditor;
    private final LongFunction<BudgetDebitor> supplyDebitor;
    private final LongUnaryOperator supplyInitialId;
    private final LongUnaryOperator supplyReplyId;
    private final LongSupplier supplyBudgetId;
    private final Http2Counters counters;
    private final Signaler signaler;
    private final Long2ObjectHashMap<Http2Server.Http2Exchange> correlations;
    private final Http2Settings initialSettings;
    private final BufferPool headersPool;
    private final int httpTypeId;
    private final MutableDirectBuffer extensionBuffer;

    Http2ServerFactory(
        Http2Configuration config,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        BudgetCreditor creditor,
        LongFunction<BudgetDebitor> supplyDebitor,
        LongUnaryOperator supplyInitialId,
        LongUnaryOperator supplyReplyId,
        LongSupplier supplyBudgetId,
        ToIntFunction<String> supplyTypeId,
        Function<String, LongSupplier> supplyCounter,
        Signaler signaler)
    {
        this.config = config;
        this.router = requireNonNull(router);
        this.writeBuffer = requireNonNull(writeBuffer);
        this.bufferPool = requireNonNull(bufferPool);
        this.creditor = creditor;
        this.supplyDebitor = supplyDebitor;
        this.supplyInitialId = requireNonNull(supplyInitialId);
        this.supplyReplyId = requireNonNull(supplyReplyId);
        this.supplyBudgetId = requireNonNull(supplyBudgetId);
        this.counters = new Http2Counters(supplyCounter);
        this.signaler = signaler;
        this.correlations = new Long2ObjectHashMap<>();
        this.headersPool = bufferPool.duplicate();
        this.initialSettings = new Http2Settings(config, headersPool);
        this.httpTypeId = supplyTypeId.applyAsInt(HttpNukleus.NAME);
        this.frameBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
        this.extensionBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
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
            final long budgetId = supplyBudgetId.getAsLong();

            final Http2Server server = new Http2Server(network, routeId, initialId, affinity, budgetId);
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

        final Http2Server.Http2Exchange exchange = correlations.remove(replyId);
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

    private int decodePreface(
        Http2Server server,
        long traceId,
        long authorization,
        long budgetId,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        final Http2PrefaceFW http2Preface = http2PrefaceRO.tryWrap(buffer, offset, limit);

        int progress = offset;
        if (http2Preface != null)
        {
            if (http2Preface.error())
            {
                server.onDecodeError(traceId, authorization, Http2ErrorCode.PROTOCOL_ERROR);
                server.decoder = decodeIgnoreAll;
            }
            else
            {
                server.onDecodePreface(traceId, authorization, http2Preface);
                progress = http2Preface.limit();
                server.decoder = decodeFrameType;
            }
        }

        return progress;
    }

    private int decodeFrameType(
        Http2Server server,
        long traceId,
        long authorization,
        long budgetId,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        final Http2FrameInfoFW http2FrameInfo = http2FrameInfoRO.tryWrap(buffer, offset, limit);

        if (http2FrameInfo != null)
        {
            final int length = http2FrameInfo.length();
            final Http2FrameType type = http2FrameInfo.type();
            final Http2ServerDecoder decoder = decodersByFrameType.getOrDefault(type, decodeIgnoreOne);

            Http2ErrorCode error = Http2ErrorCode.NO_ERROR;

            if (length > server.localSettings.maxFrameSize)
            {
                error = Http2ErrorCode.FRAME_SIZE_ERROR;
            }
            else if (decoder == null || server.continuationStreamId != 0 && decoder != decodeContinuation)
            {
                error = Http2ErrorCode.PROTOCOL_ERROR;
            }

            if (error != Http2ErrorCode.NO_ERROR)
            {
                server.onDecodeError(traceId, authorization, error);
                server.decoder = decodeIgnoreAll;
            }
            else if (limit - http2FrameInfo.limit() >= length)
            {
                server.decoder = decoder;
            }
        }

        return offset;
    }

    private int decodeSettings(
        Http2Server server,
        long traceId,
        long authorization,
        long budgetId,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        int progress = offset;

        final Http2SettingsFW http2Settings = http2SettingsRO.wrap(buffer, offset, limit);
        final int streamId = http2Settings.streamId();
        final boolean ack = http2Settings.ack();
        final int length = http2Settings.length();

        Http2ErrorCode error = Http2ErrorCode.NO_ERROR;
        if (ack && length != 0)
        {
            error = Http2ErrorCode.FRAME_SIZE_ERROR;
        }
        else if (streamId != 0)
        {
            error = Http2ErrorCode.PROTOCOL_ERROR;
        }

        if (error != Http2ErrorCode.NO_ERROR)
        {
            server.onDecodeError(traceId, authorization, error);
            server.decoder = decodeIgnoreAll;
        }
        else
        {
            counters.settingsFramesRead.getAsLong();
            server.onDecodeSettings(traceId, authorization, http2Settings);
            server.decoder = decodeFrameType;
            progress = http2Settings.limit();
        }

        return progress;
    }

    private int decodePing(
        Http2Server server,
        long traceId,
        long authorization,
        long budgetId,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        int progress = offset;

        final Http2FrameInfoFW http2FrameInfo = http2FrameInfoRO.wrap(buffer, offset, limit);
        final int streamId = http2FrameInfo.streamId();
        final int length = http2FrameInfo.length();

        Http2ErrorCode error = Http2ErrorCode.NO_ERROR;

        if (length != 8)
        {
            error = Http2ErrorCode.FRAME_SIZE_ERROR;
        }
        else if (streamId != 0)
        {
            error = Http2ErrorCode.PROTOCOL_ERROR;
        }

        if (error != Http2ErrorCode.NO_ERROR)
        {
            server.onDecodeError(traceId, authorization, error);
            server.decoder = decodeIgnoreAll;
        }
        else
        {
            final Http2PingFW http2Ping = http2PingRO.wrap(buffer, offset, limit);
            counters.pingFramesRead.getAsLong();
            server.onDecodePing(traceId, authorization, http2Ping);
            server.decoder = decodeFrameType;
            progress = http2Ping.limit();
        }

        return progress;
    }

    private int decodeGoaway(
        Http2Server server,
        long traceId,
        long authorization,
        long budgetId,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        int progress = offset;

        final Http2GoawayFW http2Goaway = http2GoawayRO.wrap(buffer, offset, limit);
        final int streamId = http2Goaway.streamId();

        Http2ErrorCode error = Http2ErrorCode.NO_ERROR;
        if (streamId != 0)
        {
            error = Http2ErrorCode.PROTOCOL_ERROR;
        }

        if (error != Http2ErrorCode.NO_ERROR)
        {
            server.onDecodeError(traceId, authorization, error);
            server.decoder = decodeIgnoreAll;
        }
        else
        {
            counters.goawayFramesRead.getAsLong();
            server.onDecodeGoaway(traceId, authorization, http2Goaway);
            server.decoder = decodeIgnoreAll;
            progress = http2Goaway.limit();
        }

        return progress;
    }

    private int decodeWindowUpdate(
        Http2Server server,
        long traceId,
        long authorization,
        long budgetId,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        int progress = offset;

        final Http2FrameInfoFW http2FrameInfo = http2FrameInfoRO.wrap(buffer, offset, limit);
        final int length = http2FrameInfo.length();

        Http2ErrorCode error = Http2ErrorCode.NO_ERROR;

        if (length != 4)
        {
            error = Http2ErrorCode.FRAME_SIZE_ERROR;
        }

        if (error == Http2ErrorCode.NO_ERROR)
        {
            final Http2WindowUpdateFW http2WindowUpdate = http2WindowUpdateRO.wrap(buffer, offset, limit);
            final int streamId = http2WindowUpdate.streamId();
            final int size = http2WindowUpdate.size();

            if (streamId == 0)
            {
                if (server.remoteSharedBudget + size > Integer.MAX_VALUE)
                {
                    error = Http2ErrorCode.FLOW_CONTROL_ERROR;
                }
            }
            else
            {
                if (streamId > server.maxClientStreamId || size < 1)
                {
                    error = Http2ErrorCode.PROTOCOL_ERROR;
                }
            }

            if (error == Http2ErrorCode.NO_ERROR)
            {
                counters.windowUpdateFramesRead.getAsLong();
                server.onDecodeWindowUpdate(traceId, authorization, http2WindowUpdate);
                server.decoder = decodeFrameType;
                progress = http2WindowUpdate.limit();
            }
        }

        if (error != Http2ErrorCode.NO_ERROR)
        {
            server.onDecodeError(traceId, authorization, error);
            server.decoder = decodeIgnoreAll;
        }

        return progress;
    }

    private int decodeHeaders(
        Http2Server server,
        long traceId,
        long authorization,
        long budgetId,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        int progress = offset;

        final Http2HeadersFW http2Headers = http2HeadersRO.wrap(buffer, offset, limit);
        final int streamId = http2Headers.streamId();

        Http2ErrorCode error = Http2ErrorCode.NO_ERROR;

        if ((streamId & 0x01) != 0x01)
        {
            error = Http2ErrorCode.PROTOCOL_ERROR;
        }

        if (error != Http2ErrorCode.NO_ERROR)
        {
            server.onDecodeError(traceId, authorization, error);
            server.decoder = decodeIgnoreAll;
        }
        else
        {
            if (server.applicationHeadersProcessed.size() < config.maxConcurrentApplicationHeaders())
            {
                counters.headersFramesRead.getAsLong();
                if (server.streams.containsKey(streamId))
                {
                    server.onDecodeTrailers(traceId, authorization, http2Headers);
                }
                else
                {
                    server.onDecodeHeaders(traceId, authorization, http2Headers);
                }
                server.decoder = decodeFrameType;
                progress = http2Headers.limit();
            }
        }

        return progress;
    }

    private int decodeContinuation(
        Http2Server server,
        long traceId,
        long authorization,
        long budgetId,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        int progress = offset;

        final Http2ContinuationFW http2Continuation = http2ContinuationRO.wrap(buffer, offset, limit);
        final int streamId = http2Continuation.streamId();
        final int length = http2Continuation.length();

        Http2ErrorCode error = Http2ErrorCode.NO_ERROR;

        if ((streamId & 0x01) != 0x01 ||
            streamId != server.continuationStreamId)
        {
            error = Http2ErrorCode.PROTOCOL_ERROR;
        }

        if (server.headersSlotOffset + length > headersPool.slotCapacity())
        {
            // TODO: decoded header list size check, recoverable error instead
            error = Http2ErrorCode.PROTOCOL_ERROR;
        }

        if (error != Http2ErrorCode.NO_ERROR)
        {
            server.onDecodeError(traceId, authorization, error);
            server.decoder = decodeIgnoreAll;
        }
        else
        {
            counters.continuationFramesRead.getAsLong();
            server.onDecodeContinuation(traceId, authorization, http2Continuation);
            server.decoder = decodeFrameType;
            progress = http2Continuation.limit();
        }

        return progress;
    }

    private int decodeData(
        Http2Server server,
        long traceId,
        long authorization,
        long budgetId,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        int progress = offset;

        final Http2DataFW http2Data = http2DataRO.wrap(buffer, offset, limit);
        final int streamId = http2Data.streamId();

        Http2ErrorCode error = Http2ErrorCode.NO_ERROR;

        if ((streamId & 0x01) != 0x01)
        {
            error = Http2ErrorCode.PROTOCOL_ERROR;
        }

        if (error != Http2ErrorCode.NO_ERROR)
        {
            server.onDecodeError(traceId, authorization, error);
            server.decoder = decodeIgnoreAll;
        }
        else
        {
            counters.dataFramesRead.getAsLong();
            server.onDecodeData(traceId, authorization, http2Data);
            server.decoder = decodeFrameType;
            progress = http2Data.limit();
        }

        return progress;
    }

    private int decodePriority(
        Http2Server server,
        long traceId,
        long authorization,
        long budgetId,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        int progress = offset;

        final Http2FrameInfoFW http2FrameInfo = http2FrameInfoRO.wrap(buffer, offset, limit);
        final int streamId = http2FrameInfo.streamId();
        final int length = http2FrameInfo.length();

        Http2ErrorCode error = Http2ErrorCode.NO_ERROR;

        if (length != 5)
        {
            error = Http2ErrorCode.FRAME_SIZE_ERROR;
        }
        else if (streamId == 0)
        {
            error = Http2ErrorCode.PROTOCOL_ERROR;
        }

        if (error != Http2ErrorCode.NO_ERROR)
        {
            server.onDecodeError(traceId, authorization, error);
            server.decoder = decodeIgnoreAll;
        }
        else
        {
            final Http2PriorityFW http2Priority = http2PriorityRO.wrap(buffer, offset, limit);
            counters.priorityFramesRead.getAsLong();
            server.onDecodePriority(traceId, authorization, http2Priority);
            server.decoder = decodeFrameType;
            progress = http2Priority.limit();
        }

        return progress;
    }

    private int decodeRstStream(
        Http2Server server,
        long traceId,
        long authorization,
        long budgetId,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        int progress = offset;

        final Http2FrameInfoFW http2FrameInfo = http2FrameInfoRO.wrap(buffer, offset, limit);
        final int streamId = http2FrameInfo.streamId();
        final int length = http2FrameInfo.length();

        Http2ErrorCode error = Http2ErrorCode.NO_ERROR;

        if (streamId == 0)
        {
            error = Http2ErrorCode.PROTOCOL_ERROR;
        }

        if (length != 4)
        {
            error = Http2ErrorCode.FRAME_SIZE_ERROR;
        }

        if (error != Http2ErrorCode.NO_ERROR)
        {
            server.onDecodeError(traceId, authorization, error);
            server.decoder = decodeIgnoreAll;
        }
        else
        {
            if (server.applicationHeadersProcessed.size() < config.maxConcurrentApplicationHeaders())
            {
                final Http2RstStreamFW http2RstStream = http2RstStreamRO.wrap(buffer, offset, limit);
                counters.resetStreamFramesRead.getAsLong();
                server.onDecodeRstStream(traceId, authorization, http2RstStream);
                server.decoder = decodeFrameType;
                progress = http2RstStream.limit();
            }
        }

        return progress;
    }

    private int decodeIgnoreOne(
        Http2Server server,
        long traceId,
        long authorization,
        long budgetId,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        final Http2FrameInfoFW http2FrameInfo = http2FrameInfoRO.wrap(buffer, offset, limit);
        final int progress = http2FrameInfo.limit() + http2FrameInfo.length();

        server.decoder = decodeFrameType;
        return progress;
    }

    private int decodeIgnoreAll(
        Http2Server server,
        long traceId,
        long authorization,
        long budgetId,
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        return limit;
    }

    private static int framePadding(
        final int dataLength,
        final int maxFrameSize)
    {
        final int frameCount = (dataLength + maxFrameSize - 1) / maxFrameSize;

        return frameCount * Http2FrameInfoFW.SIZE_OF_FRAME; // assumes H2 DATA not PADDED
    }

    @FunctionalInterface
    private interface Http2ServerDecoder
    {
        int decode(
            Http2Server server,
            long traceId,
            long authorization,
            long budgetId,
            DirectBuffer buffer,
            int offset,
            int limit);
    }

    private final class Http2Server
    {
        private final MessageConsumer network;
        private final long routeId;
        private final long initialId;
        private final long replyId;
        private final long affinity;
        private final long budgetId;

        private final Http2Settings localSettings;
        private final Http2Settings remoteSettings;
        private final HpackContext decodeContext;
        private final HpackContext encodeContext;

        private final Int2ObjectHashMap<Http2Exchange> streams;
        private final LongHashSet applicationHeadersProcessed;
        private final int[] streamsActive = new int[2];

        private final MutableBoolean expectDynamicTableSizeUpdate = new MutableBoolean(true);

        private int initialBudget;
        private int replyPadding;
        private int replyBudget;

        private int replyBudgetReserved;
        private int replySharedBudget;

        private int remoteSharedBudget;
        private long responseSharedBudgetIndex = NO_CREDITOR_INDEX;
        private int responseSharedBudget;

        private int decodeSlot = NO_SLOT;
        private int decodeSlotOffset;
        private int decodeSlotReserved;

        private int encodeSlot = NO_SLOT;
        private int encodeSlotOffset;
        private int encodeSlotReserved;
        private int encodeSlotMarkOffset;
        private int encodeHeadersSlotMarkOffset;
        private int encodeReservedSlotMarkOffset;

        private MutableDirectBuffer encodeHeadersBuffer;
        private int encodeHeadersSlotOffset;
        private long encodeHeadersSlotTraceId;

        private MutableDirectBuffer encodeReservedBuffer;
        private int encodeReservedSlotOffset;
        private long encodeReservedSlotTraceId;

        private int headersSlot = NO_SLOT;
        private int headersSlotOffset;

        private Http2ServerDecoder decoder;

        private int state;
        private int maxClientStreamId;
        private int maxServerStreamId;
        private int continuationStreamId;
        private Http2ErrorCode decodeError;
        private LongLongConsumer cleanupHandler;

        private Http2Server(
            MessageConsumer network,
            long routeId,
            long initialId,
            long affinity,
            long budgetId)
        {
            this.network = network;
            this.routeId = routeId;
            this.initialId = initialId;
            this.affinity = affinity;
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.budgetId = budgetId;
            this.localSettings = new Http2Settings();
            this.remoteSettings = new Http2Settings();
            this.streams = new Int2ObjectHashMap<>();
            this.applicationHeadersProcessed = new LongHashSet();
            this.decoder = decodePreface;
            this.decodeContext = new HpackContext(localSettings.headerTableSize, false);
            this.encodeContext = new HpackContext(remoteSettings.headerTableSize, true);
            this.encodeHeadersBuffer = new ExpandableArrayBuffer();
            this.encodeReservedBuffer = new ExpandableArrayBuffer();
            this.remoteSharedBudget = remoteSettings.initialWindowSize;
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

            doNetworkWindow(traceId, authorization, bufferPool.slotCapacity(), 0, 0);
            state = Http2State.openInitial(state);
            doNetworkBegin(traceId, authorization);
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
                DirectBuffer buffer = payload.buffer();
                int offset = payload.offset();
                int limit = payload.limit();
                int reserved = data.reserved();

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

                final int initialCredit = reserved - decodeSlotReserved;
                if (initialCredit > 0)
                {
                    doNetworkWindow(traceId, authorization, initialCredit, 0, 0);
                }
            }
        }

        private void onNetworkEnd(
            EndFW end)
        {
            if (decodeSlot == NO_SLOT)
            {
                final long traceId = end.traceId();
                final long authorization = end.authorization();
                state = Http2State.closeInitial(state);

                cleanupDecodeSlotIfNecessary();

                if (!Http2State.replyClosing(state))
                {
                    state = Http2State.closingReply(state);
                    cleanup(traceId, authorization, this::doNetworkEnd);
                }
            }

            decoder = decodeIgnoreAll;
        }

        private void onNetworkSignal(
            long traceId,
            long authorization,
            int signalId)
        {
            assert signalId == CLEANUP_SIGNAL;
            cleanupStreams(traceId, authorization);
        }

        private void cleanup(
            long traceId,
            long authorization,
            LongLongConsumer cleanupHandler)
        {
            assert this.cleanupHandler == null;
            this.cleanupHandler = cleanupHandler;
            cleanupStreams(traceId, authorization);
        }

        private void cleanupStreams(
            long traceId,
            long authorization)
        {
            int remaining = config.maxConcurrentStreamsCleanup();
            for (Iterator<Http2Exchange> iterator = streams.values().iterator();
                 iterator.hasNext() && remaining > 0; remaining--)
            {
                final Http2Exchange stream = iterator.next();
                stream.cleanup(traceId, authorization);
            }

            if (!streams.isEmpty())
            {
                final long timeMillis = Instant.now().plusMillis(config.streamsCleanupDelay()).toEpochMilli();
                signaler.signalAt(timeMillis, CLEANUP_SIGNAL, s -> onNetworkSignal(traceId, authorization, s));
            }
            else
            {
                cleanupHandler.accept(traceId, authorization);
            }
        }

        private void onNetworkAbort(
            AbortFW abort)
        {
            final long traceId = abort.traceId();
            final long authorization = abort.authorization();
            state = Http2State.closeInitial(state);

            cleanupDecodeSlotIfNecessary();

            if (!Http2State.replyClosing(state))
            {
                state = Http2State.closingReply(state);
                cleanup(traceId, authorization, this::doNetworkAbort);
            }
        }

        private void onNetworkReset(
            ResetFW reset)
        {
            final long traceId = reset.traceId();
            final long authorization = reset.authorization();
            state = Http2State.closeReply(state);

            cleanupBudgetCreditorIfNecessary();
            cleanupEncodeSlotIfNecessary();

            if (!Http2State.initialClosing(state))
            {
                state = Http2State.closingInitial(state);
                cleanup(traceId, authorization, this::doNetworkReset);
            }
        }

        private void onNetworkWindow(
            WindowFW window)
        {
            final long traceId = window.traceId();
            final long authorization = window.authorization();
            final long budgetId = window.budgetId();
            int credit = window.credit();
            final int padding = window.padding();

            if (Http2Configuration.DEBUG_HTTP2_BUDGETS)
            {
                System.out.format("[%d] [onNetworkWindow] [0x%016x] [0x%016x] replyBudget %d + %d => %d\n",
                    System.nanoTime(), encodeReservedSlotTraceId, budgetId,
                    replyBudget, credit, replyBudget + credit);
            }

            replyBudget += credit;
            replyPadding = padding;

            if (replyBudgetReserved > 0)
            {
                final int reservedCredit = Math.min(credit, replyBudgetReserved);
                replyBudgetReserved -= reservedCredit;
                credit -= reservedCredit;
            }

            if (credit > 0)
            {
                replySharedBudget += credit;
                credit -= credit;
            }

            assert credit == 0;

            encodeNetwork(traceId, authorization, budgetId);

            flushResponseSharedBudget(traceId);
        }

        private void doNetworkBegin(
            long traceId,
            long authorization)
        {
            doBegin(network, routeId, replyId, traceId, authorization, affinity, EMPTY_OCTETS);
            router.setThrottle(replyId, this::onNetwork);

            assert responseSharedBudgetIndex == NO_CREDITOR_INDEX;
            responseSharedBudgetIndex = creditor.acquire(budgetId);
            state = Http2State.openReply(state);
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
                encodeBuffer.putBytes(encodeSlotOffset, buffer, offset, limit - offset);
                encodeSlotOffset += limit - offset;
                encodeSlotReserved += reserved;

                if (Http2Configuration.DEBUG_HTTP2_BUDGETS)
                {
                    System.out.format("[%d] [doNetworkData] [0x%016x] [0x%016x] encodeSlotOffset %d => %d " +
                                      "encodeSlotReserved=%d\n",
                        System.nanoTime(), traceId, budgetId, limit - offset, encodeSlotOffset, encodeSlotReserved);
                }

                encodeNetwork(traceId, authorization, budgetId);
            }

        }

        private void doNetworkHeadersData(
            long traceId,
            long authorization,
            long budgetId,
            Flyweight payload)
        {
            doNetworkHeadersData(traceId, authorization, budgetId, payload.buffer(),
                payload.offset(), payload.limit());
        }

        private void doNetworkHeadersData(
            long traceId,
            long authorization,
            long budgetId,
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            if (encodeHeadersSlotOffset == 0)
            {
                encodeSlotMarkOffset = encodeSlotOffset;
                assert encodeSlotMarkOffset >= 0;

                encodeReservedSlotMarkOffset = encodeReservedSlotOffset;
                assert encodeReservedSlotMarkOffset >= 0;
            }

            encodeHeadersBuffer.putBytes(encodeHeadersSlotOffset, buffer, offset, limit - offset);
            encodeHeadersSlotOffset += limit - offset;
            encodeHeadersSlotTraceId = traceId;

            encodeNetwork(traceId, authorization, budgetId);
        }

        private void doNetworkReservedData(
            long traceId,
            long authorization,
            long budgetId,
            Flyweight payload)
        {
            doNetworkReservedData(traceId, authorization, budgetId, payload.buffer(),
                payload.offset(), payload.limit());
        }

        private void doNetworkReservedData(
            long traceId,
            long authorization,
            long budgetId,
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            encodeReservedBuffer.putBytes(encodeReservedSlotOffset, buffer, offset, limit - offset);
            encodeReservedSlotOffset += limit - offset;
            encodeReservedSlotTraceId = traceId;

            encodeNetwork(traceId, authorization, budgetId);
        }

        private void doNetworkEnd(
            long traceId,
            long authorization)
        {
            cleanupBudgetCreditorIfNecessary();
            cleanupEncodeSlotIfNecessary();
            doEnd(network, routeId, replyId, traceId, authorization, EMPTY_OCTETS);
            state = Http2State.closeReply(state);
        }

        private void doNetworkAbort(
            long traceId,
            long authorization)
        {
            cleanupBudgetCreditorIfNecessary();
            cleanupEncodeSlotIfNecessary();
            doAbort(network, routeId, replyId, traceId, authorization, EMPTY_OCTETS);
            state = Http2State.closeReply(state);
        }

        private void doNetworkReset(
            long traceId,
            long authorization)
        {
            cleanupDecodeSlotIfNecessary();
            cleanupHeadersSlotIfNecessary();
            doReset(network, routeId, initialId, traceId, authorization);
            state = Http2State.closeInitial(state);
        }

        private void doNetworkWindow(
            long traceId,
            long authorization,
            int credit,
            int padding,
            long budgetId)
        {
            assert credit > 0;

            initialBudget += credit;
            assert initialBudget <= bufferPool.slotCapacity();
            doWindow(network, routeId, initialId, traceId, authorization, budgetId, credit, padding);
        }

        private void encodeNetwork(
            long traceId,
            long authorization,
            long budgetId)
        {
            encodeNetworkHeaders(authorization, budgetId);
            encodeNetworkData(traceId, authorization, budgetId);
            encodeNetworkReserved(authorization, budgetId);
        }

        private void encodeNetworkData(
            long traceId,
            long authorization,
            long budgetId)
        {
            if (encodeSlotOffset != 0 &&
                (encodeSlotMarkOffset != 0 || (encodeHeadersSlotOffset == 0 && encodeReservedSlotMarkOffset == 0)))
            {
                final int encodeLengthMax = encodeSlotMarkOffset != 0 ? encodeSlotMarkOffset : encodeSlotOffset;
                final int encodeLength = Math.max(Math.min(replyBudget - replyPadding, encodeLengthMax), 0);

                if (encodeLength > 0)
                {
                    final int encodeReserved = encodeLength + replyPadding;
                    final int encodeReservedMin = (int) (((long) encodeSlotReserved * encodeLength) / encodeSlotOffset);

                    if (Http2Configuration.DEBUG_HTTP2_BUDGETS)
                    {
                        System.out.format("[%d] [encodeNetworkData] [0x%016x] [0x%016x] replyBudget %d - %d => %d\n",
                            System.nanoTime(), traceId, budgetId,
                            replyBudget, encodeReserved, replyBudget - encodeReserved);

                        System.out.format("[%d] [encodeNetworkData] [0x%016x]  [0x%016x] replySharedBudget %d - %d => %d\n",
                            System.nanoTime(), traceId, budgetId,
                            replySharedBudget, encodeReservedMin, replySharedBudget - encodeReservedMin);
                    }

                    replyBudget -= encodeReserved;
                    assert replyBudget >= 0 : String.format("%d >= 0", replyBudget);

                    replySharedBudget -= encodeReserved;
                    encodeSlotReserved -= encodeReservedMin;

                    assert encodeSlot != NO_SLOT;
                    final MutableDirectBuffer encodeBuffer = bufferPool.buffer(encodeSlot);

                    doData(network, routeId, replyId, traceId, authorization, budgetId,
                        encodeReserved, encodeBuffer, 0, encodeLength, EMPTY_OCTETS);

                    if (encodeSlotMarkOffset != 0)
                    {
                        encodeSlotMarkOffset -= encodeLength;
                        assert encodeSlotMarkOffset >= 0;
                    }

                    encodeSlotOffset -= encodeLength;
                    assert encodeSlotOffset >= 0;

                    if (encodeSlotOffset > 0)
                    {
                        encodeBuffer.putBytes(0, encodeBuffer, encodeLength, encodeSlotOffset);

                        if (encodeSlotMarkOffset == 0 && encodeHeadersSlotOffset == 0)
                        {
                            encodeSlotMarkOffset += encodeSlotOffset;
                        }
                    }
                    else
                    {
                        cleanupEncodeSlotIfNecessary();

                        if (streams.isEmpty() && decoder == decodeIgnoreAll)
                        {
                            doNetworkEnd(traceId, authorization);
                        }
                    }
                }
            }
        }

        private void encodeNetworkHeaders(
            long authorization,
            long budgetId)
        {
            if (encodeHeadersSlotOffset != 0 &&
                encodeSlotMarkOffset == 0 &&
                encodeReservedSlotMarkOffset == 0)
            {
                final int maxEncodeLength =
                    encodeHeadersSlotMarkOffset != 0 ? encodeHeadersSlotMarkOffset : encodeHeadersSlotOffset;
                final int encodeLength = Math.max(Math.min(replyBudget - replyPadding, maxEncodeLength), 0);

                if (encodeLength > 0)
                {
                    final int encodeReserved = encodeLength + replyPadding;

                    if (Http2Configuration.DEBUG_HTTP2_BUDGETS)
                    {
                        System.out.format("[%d] [encodeNetworkHeaders] [0x%016x] [0x%016x] replyBudget %d - %d => %d\n",
                            System.nanoTime(), encodeHeadersSlotTraceId, budgetId,
                            replyBudget, encodeReserved, replyBudget - encodeReserved);
                    }

                    replyBudget -= encodeReserved;
                    assert replyBudget >= 0;

                    doData(network, routeId, replyId, encodeHeadersSlotTraceId, authorization, budgetId,
                        encodeReserved, encodeHeadersBuffer, 0, encodeLength, EMPTY_OCTETS);

                    if (encodeHeadersSlotMarkOffset != 0)
                    {
                        encodeHeadersSlotMarkOffset -= encodeLength;
                        assert encodeHeadersSlotMarkOffset >= 0;
                    }

                    encodeHeadersSlotOffset -= encodeLength;
                    assert encodeHeadersSlotOffset >= 0;

                    if (encodeHeadersSlotOffset > 0)
                    {
                        encodeHeadersBuffer.putBytes(0, encodeHeadersBuffer, encodeLength, encodeHeadersSlotOffset);

                        if (encodeHeadersSlotMarkOffset == 0)
                        {
                            encodeHeadersSlotMarkOffset = encodeHeadersSlotOffset;
                        }
                    }

                    replyBudgetReserved += encodeReserved;
                }
            }
        }

        private void encodeNetworkReserved(
            long authorization,
            long budgetId)
        {
            if (encodeReservedSlotOffset != 0 &&
                (encodeReservedSlotMarkOffset != 0 || (encodeHeadersSlotOffset == 0 && encodeSlotOffset == 0)))
            {
                final int maxEncodeLength =
                    encodeReservedSlotMarkOffset != 0 ? encodeReservedSlotMarkOffset : encodeReservedSlotOffset;
                final int encodeLength = Math.max(Math.min(replyBudget - replyPadding, maxEncodeLength), 0);

                if (encodeLength > 0)
                {
                    final int encodeReserved = encodeLength + replyPadding;

                    if (Http2Configuration.DEBUG_HTTP2_BUDGETS)
                    {
                        System.out.format("[%d] [encodeNetworkReserved] [0x%016x] [0x%016x] replyBudget %d - %d => %d\n",
                            System.nanoTime(), encodeReservedSlotTraceId, budgetId,
                            replyBudget, encodeReserved, replyBudget - encodeReserved);
                    }

                    replyBudget -= encodeReserved;
                    assert replyBudget >= 0;

                    doData(network, routeId, replyId, encodeReservedSlotTraceId, authorization, budgetId,
                        encodeReserved, encodeReservedBuffer, 0, encodeLength, EMPTY_OCTETS);

                    if (encodeReservedSlotMarkOffset != 0)
                    {
                        encodeReservedSlotMarkOffset -= encodeLength;
                        assert encodeReservedSlotMarkOffset >= 0;
                    }

                    encodeReservedSlotOffset -= encodeLength;
                    assert encodeReservedSlotOffset >= 0;

                    if (encodeReservedSlotOffset > 0)
                    {
                        encodeReservedBuffer.putBytes(0, encodeReservedBuffer, encodeLength, encodeReservedSlotOffset);

                        if (encodeReservedSlotMarkOffset == 0 &&
                            encodeHeadersSlotOffset == 0 &&
                            encodeSlotOffset == 0)
                        {
                            encodeReservedSlotMarkOffset = encodeReservedSlotOffset;
                        }
                    }

                    replyBudgetReserved += encodeReserved;
                }
            }
        }

        private void decodeNetworkIfNecessary(
            long traceId,
            long authorization)
        {
            if (decodeSlot != NO_SLOT)
            {
                final MutableDirectBuffer decodeBuffer = bufferPool.buffer(decodeSlot);
                final int offset = 0;
                final int limit = decodeSlotOffset;
                final int reserved = decodeSlotReserved;

                decodeNetwork(traceId, authorization, budgetId, reserved, decodeBuffer, offset, limit);

                final int initialCredit = reserved - decodeSlotReserved;
                if (initialCredit > 0)
                {
                    doNetworkWindow(traceId, authorization, initialCredit, 0, 0);
                }
            }
        }

        private int decodeNetwork(
            long traceId,
            long authorization,
            long budgetId,
            int reserved,
            DirectBuffer buffer,
            int offset,
            int limit)
        {
            Http2ServerDecoder previous = null;
            int progress = offset;
            while (progress <= limit && previous != decoder)
            {
                previous = decoder;
                progress = decoder.decode(this, traceId, authorization, budgetId, buffer, progress, limit);
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
                    decodeSlotReserved = (int)((long) reserved * (limit - progress) / (limit - offset));
                }
            }
            else
            {
                cleanupDecodeSlotIfNecessary();
            }

            return progress;
        }

        private void onDecodeError(
            long traceId,
            long authorization,
            Http2ErrorCode error)
        {
            this.decodeError = error;
            cleanup(traceId, authorization, this::doEncodeGoaway);
        }

        private void onDecodePreface(
            long traceId,
            long authorization,
            Http2PrefaceFW http2Preface)
        {
            doEncodeSettings(traceId, authorization);
        }

        private void onDecodeSettings(
            long traceId,
            long authorization,
            Http2SettingsFW http2Settings)
        {
            if (http2Settings.ack())
            {
                final int localInitialCredit = initialSettings.initialWindowSize - localSettings.initialWindowSize;

                // initial budget can become negative
                if (localInitialCredit != 0)
                {
                    for (Http2Exchange stream: streams.values())
                    {
                        stream.localBudget += localInitialCredit;
                    }
                }

                localSettings.apply(initialSettings);
            }
            else
            {
                final int remoteInitialBudget = remoteSettings.initialWindowSize;
                http2Settings.forEach(this::onDecodeSetting);

                Http2ErrorCode decodeError = remoteSettings.error();

                if (decodeError == Http2ErrorCode.NO_ERROR)
                {
                    // reply budget can become negative
                    final long remoteInitialCredit = remoteSettings.initialWindowSize - remoteInitialBudget;
                    if (remoteInitialCredit != 0)
                    {
                        for (Http2Exchange stream: streams.values())
                        {
                            final long newRemoteBudget = stream.remoteBudget + remoteInitialCredit;
                            if (newRemoteBudget > MAX_REMOTE_BUDGET)
                            {
                                decodeError = Http2ErrorCode.FLOW_CONTROL_ERROR;
                                break;
                            }
                            stream.remoteBudget = (int) newRemoteBudget;
                        }
                    }
                }

                if (decodeError == Http2ErrorCode.NO_ERROR)
                {
                    doEncodeSettingsAck(traceId, authorization);
                }
                else
                {
                    onDecodeError(traceId, authorization, decodeError);
                    decoder = decodeIgnoreAll;
                }
            }
        }

        private void onDecodeSetting(
            Http2Setting setting,
            int value)
        {
            switch (setting)
            {
            case HEADER_TABLE_SIZE:
                remoteSettings.headerTableSize = value;
                break;
            case ENABLE_PUSH:
                remoteSettings.enablePush = value;
                break;
            case MAX_CONCURRENT_STREAMS:
                remoteSettings.maxConcurrentStreams = value;
                break;
            case INITIAL_WINDOW_SIZE:
                remoteSettings.initialWindowSize = value;
                break;
            case MAX_FRAME_SIZE:
                remoteSettings.maxFrameSize = value;
                break;
            case MAX_HEADER_LIST_SIZE:
                remoteSettings.maxHeaderListSize = value;
                break;
            case UNKNOWN:
                break;
            }
        }

        private void onDecodePing(
            long traceId,
            long authorization,
            Http2PingFW http2Ping)
        {
            if (!http2Ping.ack())
            {
                doEncodePingAck(traceId, authorization, http2Ping.payload());
            }
        }

        private void onDecodeGoaway(
            long traceId,
            long authorization,
            Http2GoawayFW http2Goaway)
        {
            final int lastStreamId = http2Goaway.lastStreamId();

            streams.entrySet()
                   .stream()
                   .filter(e -> e.getKey() > lastStreamId)
                   .map(Map.Entry::getValue)
                   .forEach(ex -> ex.cleanup(traceId, authorization));

            remoteSettings.enablePush = 0;
        }

        private void onDecodeWindowUpdate(
            long traceId,
            long authorization,
            Http2WindowUpdateFW http2WindowUpdate)
        {
            final int streamId = http2WindowUpdate.streamId();
            final int credit = http2WindowUpdate.size();

            if (streamId == 0)
            {
                if (Http2Configuration.DEBUG_HTTP2_BUDGETS)
                {
                    System.out.format("[%d] [onDecodeWindowUpdate] [0x%016x] [0x%016x] %d + %d => %d \n",
                        System.nanoTime(), traceId, budgetId, remoteSharedBudget, credit, remoteSharedBudget + credit);
                }

                remoteSharedBudget += credit;

                // TODO: instead use Http2State.replyClosed(state)
                if (responseSharedBudgetIndex != NO_CREDITOR_INDEX)
                {
                    flushResponseSharedBudget(traceId);
                }
            }
            else
            {
                final Http2Exchange stream = streams.get(streamId);
                if (stream != null)
                {
                    stream.onResponseWindowUpdate(traceId, authorization, credit);
                }
            }
        }

        private void onDecodeHeaders(
            long traceId,
            long authorization,
            Http2HeadersFW http2Headers)
        {
            final int streamId = http2Headers.streamId();
            final int parentStreamId = http2Headers.parentStream();
            final int dataLength = http2Headers.dataLength();

            Http2ErrorCode error = Http2ErrorCode.NO_ERROR;

            if (streamId <= maxClientStreamId ||
                parentStreamId == streamId ||
                dataLength < 0)
            {
                error = Http2ErrorCode.PROTOCOL_ERROR;
            }

            maxClientStreamId = streamId;

            if (streamsActive[CLIENT_INITIATED] >= localSettings.maxConcurrentStreams)
            {
                error = Http2ErrorCode.REFUSED_STREAM;
            }

            if (error != Http2ErrorCode.NO_ERROR)
            {
                doEncodeRstStream(traceId, authorization, streamId, error);
            }

            final DirectBuffer dataBuffer = http2Headers.buffer();
            final int dataOffset = http2Headers.dataOffset();

            final boolean endHeaders = http2Headers.endHeaders();
            final boolean endRequest = http2Headers.endStream();

            if (endHeaders)
            {
                onDecodeHeaders(traceId, authorization, streamId, dataBuffer, dataOffset, dataOffset + dataLength, endRequest);
            }
            else
            {
                assert headersSlot == NO_SLOT;
                assert headersSlotOffset == 0;

                headersSlot = headersPool.acquire(initialId);
                if (headersSlot == NO_SLOT)
                {
                    cleanupNetwork(traceId, authorization);
                }
                else
                {
                    final MutableDirectBuffer headersBuffer = headersPool.buffer(headersSlot);
                    headersBuffer.putBytes(headersSlotOffset, dataBuffer, dataOffset, dataLength);
                    headersSlotOffset = dataLength;

                    continuationStreamId = streamId;
                }
            }
        }

        private void onDecodeContinuation(
            long traceId,
            long authorization,
            Http2ContinuationFW http2Continuation)
        {
            assert headersSlot != NO_SLOT;
            assert headersSlotOffset != 0;

            final int streamId = http2Continuation.streamId();
            final DirectBuffer payload = http2Continuation.payload();
            final boolean endHeaders = http2Continuation.endHeaders();
            final boolean endRequest = http2Continuation.endStream();

            final MutableDirectBuffer headersBuffer = headersPool.buffer(headersSlot);
            headersBuffer.putBytes(headersSlotOffset, payload, 0, payload.capacity());
            headersSlotOffset += payload.capacity();

            if (endHeaders)
            {
                if (streams.containsKey(streamId))
                {
                    onDecodeTrailers(traceId, authorization, streamId, headersBuffer, 0, headersSlotOffset, endRequest);
                }
                else
                {
                    onDecodeHeaders(traceId, authorization, streamId, headersBuffer, 0, headersSlotOffset, endRequest);
                }
                continuationStreamId = 0;

                cleanupHeadersSlotIfNecessary();
            }
        }

        private void onDecodeHeaders(
            long traceId,
            long authorization,
            int streamId,
            DirectBuffer buffer,
            int offset,
            int limit,
            boolean endRequest)
        {
            final HpackHeaderBlockFW headerBlock = headerBlockRO.wrap(buffer, offset, limit);
            headersDecoder.decodeHeaders(decodeContext, localSettings.headerTableSize, expectDynamicTableSizeUpdate, headerBlock);

            if (headersDecoder.error())
            {
                if (headersDecoder.streamError != null)
                {
                    doEncodeRstStream(traceId, authorization, streamId, headersDecoder.streamError);
                }
                else if (headersDecoder.connectionError != null)
                {
                    onDecodeError(traceId, authorization, headersDecoder.connectionError);
                    decoder = decodeIgnoreAll;
                }
            }
            else if (headersDecoder.httpError())
            {
                doEncodeHeaders(traceId, authorization, streamId, headersDecoder.httpErrorHeader, true);
            }
            else
            {
                final Map<String, String> headers = headersDecoder.headers;
                final String authority = headers.get(":authority");
                if (authority.indexOf(':') == -1)
                {
                    String scheme = headers.get(":scheme");
                    String defaultPort = "https".equals(scheme) ? ":443" : ":80";
                    headers.put(":authority", authority + defaultPort);
                }

                final MessagePredicate filter = (t, b, o, l) ->
                {
                    final RouteFW route = routeRO.wrap(b, o, o + l);
                    final HttpRouteExFW routeEx = route.extension().get(routeExRO::tryWrap);

                    return routeEx == null || matchHeaders(headers, routeEx.headers());
                };

                final RouteFW route = router.resolve(routeId, authorization, filter, wrapRoute);
                if (route == null)
                {
                    doEncodeHeaders(traceId, authorization, streamId, HEADERS_404_NOT_FOUND, true);
                }
                else
                {
                    final HttpRouteExFW routeEx = route.extension().get(routeExRO::tryWrap);
                    if (routeEx != null)
                    {
                        routeEx.overrides().forEach(h -> headers.put(h.name().asString(), h.value().asString()));
                    }

                    final long routeId = route.correlationId();
                    final long contentLength = headersDecoder.contentLength;

                    final Http2Exchange exchange = new Http2Exchange(routeId, streamId, contentLength);

                    final HttpBeginExFW beginEx = beginExRW.wrap(extensionBuffer, 0, extensionBuffer.capacity())
                                                   .typeId(httpTypeId)
                                                   .headers(hs -> headers.forEach((n, v) -> hs.item(h -> h.name(n).value(v))))
                                                   .build();

                    exchange.doRequestBegin(traceId, authorization, beginEx);
                    correlations.put(exchange.responseId, exchange);

                    if (endRequest)
                    {
                        exchange.doRequestEnd(traceId, authorization, EMPTY_OCTETS);
                    }
                }
            }
        }

        private boolean matchHeaders(
            Map<String, String> beginHeaders,
            Array32FW<HttpHeaderFW> routeHeaders)
        {
            final Predicate<HttpHeaderFW> matcher = r ->
            {
                final String8FW name = r.name();
                final String value = r.value().asString();
                final String candidate = beginHeaders.get(name.asString());
                return Objects.equals(value, candidate) ||
                       (candidate != null &&
                        HEADER_PATH.equals(name) &&
                        value.endsWith("/") &&
                        candidate.startsWith(value));
            };

            final MutableBoolean match = new MutableBoolean(true);
            routeHeaders.forEach(r -> match.value &= matcher.test(r));
            return match.value;
        }

        private void onDecodeTrailers(
            long traceId,
            long authorization,
            Http2HeadersFW http2Trailers)
        {
            final int streamId = http2Trailers.parentStream();
            final int dataLength = http2Trailers.dataLength();

            Http2ErrorCode error = Http2ErrorCode.NO_ERROR;

            if (dataLength < 0)
            {
                error = Http2ErrorCode.PROTOCOL_ERROR;
            }

            if (error != Http2ErrorCode.NO_ERROR)
            {
                doEncodeRstStream(traceId, authorization, streamId, error);
            }

            final DirectBuffer dataBuffer = http2Trailers.buffer();
            final int dataOffset = http2Trailers.dataOffset();

            final boolean endHeaders = http2Trailers.endHeaders();
            final boolean endRequest = http2Trailers.endStream();

            if (endHeaders)
            {
                onDecodeTrailers(traceId, authorization, streamId, dataBuffer, dataOffset, dataOffset + dataLength, endRequest);
            }
            else
            {
                assert headersSlot == NO_SLOT;
                assert headersSlotOffset == 0;

                headersSlot = headersPool.acquire(initialId);
                if (headersSlot == NO_SLOT)
                {
                    cleanupNetwork(traceId, authorization);
                }
                else
                {
                    final MutableDirectBuffer headersBuffer = headersPool.buffer(headersSlot);
                    headersBuffer.putBytes(headersSlotOffset, dataBuffer, dataOffset, dataLength);
                    headersSlotOffset = dataLength;

                    continuationStreamId = streamId;
                }
            }
        }

        private void onDecodeTrailers(
            long traceId,
            long authorization,
            int streamId,
            DirectBuffer buffer,
            int offset,
            int limit,
            boolean endRequest)
        {
            final Http2Exchange exchange = streams.get(streamId);
            if (exchange != null)
            {
                final HpackHeaderBlockFW headerBlock = headerBlockRO.wrap(buffer, offset, limit);
                headersDecoder.decodeTrailers(decodeContext, localSettings.headerTableSize,
                    expectDynamicTableSizeUpdate, headerBlock);

                if (headersDecoder.error())
                {
                    if (headersDecoder.streamError != null)
                    {
                        doEncodeRstStream(traceId, authorization, streamId, headersDecoder.streamError);
                        exchange.cleanup(traceId, authorization);
                    }
                    else if (headersDecoder.connectionError != null)
                    {
                        onDecodeError(traceId, authorization, headersDecoder.connectionError);
                        decoder = decodeIgnoreAll;
                    }
                }
                else
                {
                    final Map<String, String> trailers = headersDecoder.headers;
                    final HttpEndExFW endEx = endExRW.wrap(extensionBuffer, 0, extensionBuffer.capacity())
                                                     .typeId(httpTypeId)
                                                     .trailers(ts -> trailers.forEach((n, v) -> ts.item(t -> t.name(n).value(v))))
                                                     .build();

                    exchange.doRequestEnd(traceId, authorization, endEx);
                }
            }
        }

        private void onDecodeData(
            long traceId,
            long authorization,
            Http2DataFW http2Data)
        {
            final int streamId = http2Data.streamId();
            final int dataLength = http2Data.dataLength();
            final boolean endRequest = http2Data.endStream();

            final Http2Exchange exchange = streams.get(streamId);
            if (exchange != null)
            {
                Http2ErrorCode error = Http2ErrorCode.NO_ERROR;

                if (Http2State.initialClosing(exchange.state) || dataLength < 0)
                {
                    error = Http2ErrorCode.STREAM_CLOSED;
                }

                if (error != Http2ErrorCode.NO_ERROR)
                {
                    exchange.cleanup(traceId, authorization);
                    doEncodeRstStream(traceId, authorization, streamId, error);
                }
                else
                {
                    if (dataLength > 0)
                    {
                        final DirectBuffer payload = http2Data.payload();
                        exchange.doRequestData(traceId, authorization, payload, 0, payload.capacity());
                    }

                    if (endRequest)
                    {
                        if (exchange.contentLength != -1 && exchange.contentObserved != exchange.contentLength)
                        {
                            doEncodeRstStream(traceId, authorization, streamId, Http2ErrorCode.PROTOCOL_ERROR);
                        }
                        else
                        {
                            exchange.doRequestEnd(traceId, authorization, EMPTY_OCTETS);
                        }
                    }
                }
            }
        }

        private void onDecodePriority(
            long traceId,
            long authorization,
            Http2PriorityFW http2Priority)
        {
            final int streamId = http2Priority.streamId();
            final int parentStream = http2Priority.parentStream();

            final Http2Exchange exchange = streams.get(streamId);
            if (exchange != null)
            {
                if (parentStream == streamId)
                {
                    doEncodeRstStream(traceId, authorization, streamId, Http2ErrorCode.PROTOCOL_ERROR);
                }
            }
        }

        private void onDecodeRstStream(
            long traceId,
            long authorization,
            Http2RstStreamFW http2RstStream)
        {
            final int streamId = http2RstStream.streamId();
            final Http2Exchange exchange = streams.get(streamId);

            if (exchange != null)
            {
                exchange.cleanup(traceId, authorization);
            }
        }

        private void flushResponseSharedBudget(
            long traceId)
        {
            final int slotCapacity = bufferPool.slotCapacity();
            final int responseSharedPadding = framePadding(remoteSharedBudget, remoteSettings.maxFrameSize);
            final int remoteSharedBudgetMax = remoteSharedBudget + responseSharedPadding + replyPadding;
            final int responseSharedCredit =
                Math.min(slotCapacity - responseSharedBudget - encodeSlotReserved, replySharedBudget);
            final int responseSharedBudgetDelta = remoteSharedBudgetMax - (responseSharedBudget + encodeSlotReserved);
            final int replySharedCredit = Math.min(responseSharedCredit, responseSharedBudgetDelta);

            if (replySharedCredit > 0)
            {
                final long responseSharedPrevious =
                    creditor.credit(traceId, responseSharedBudgetIndex, replySharedCredit);

                if (Http2Configuration.DEBUG_HTTP2_BUDGETS)
                {
                    System.out.format("[%d] [flushResponseSharedBudget] [0x%016x] [0x%016x] " +
                                      "responseSharedBudget %d + %d => %d\n",
                        System.nanoTime(), traceId, budgetId,
                        responseSharedBudget, replySharedCredit, responseSharedBudget + replySharedCredit);
                }

                responseSharedBudget += replySharedCredit;

                final long responseSharedBudgetUpdated = responseSharedPrevious + replySharedCredit;
                assert responseSharedBudgetUpdated <= slotCapacity
                    : String.format("%d <= %d, remoteSharedBudget = %d",
                    responseSharedBudgetUpdated, slotCapacity, remoteSharedBudget);

                assert responseSharedBudget <= slotCapacity
                    : String.format("%d <= %d", responseSharedBudget, slotCapacity);

                assert replySharedBudget <= slotCapacity
                    : String.format("%d <= %d", replySharedBudget, slotCapacity);
            }
        }

        private void onEncodePromise(
            long traceId,
            long authorization,
            int streamId,
            Array32FW<HttpHeaderFW> promise)
        {
            final Map<String, String> headers = headersDecoder.headers;
            headers.clear();
            promise.forEach(h -> headers.put(h.name().asString(), h.value().asString()));

            final MessagePredicate filter = (t, b, o, l) ->
            {
                final RouteFW route = routeRO.wrap(b, o, o + l);
                final HttpRouteExFW routeEx = route.extension().get(routeExRO::tryWrap);

                return routeEx == null || matchHeaders(headers, routeEx.headers());
            };

            final RouteFW route = router.resolve(routeId, authorization, filter, wrapRoute);
            if (route != null)
            {
                final int pushId =
                    remoteSettings.enablePush == 1 &&
                    streamsActive[SERVER_INITIATED] < remoteSettings.maxConcurrentStreams
                        ? (streamId & 0x01) == CLIENT_INITIATED
                        ? streamId
                        : streams.entrySet()
                                 .stream()
                                 .map(Map.Entry::getValue)
                                 .filter(ex -> (ex.streamId & 0x01) == CLIENT_INITIATED)
                                 .filter(Http2Exchange::isResponseOpen)
                                 .mapToInt(ex -> ex.streamId)
                                 .findAny()
                                 .orElse(-1)
                        : -1;

                if (pushId != -1)
                {
                    final HttpRouteExFW routeEx = route.extension().get(routeExRO::tryWrap);
                    if (routeEx != null)
                    {
                        routeEx.overrides().forEach(h -> headers.put(h.name().asString(), h.value().asString()));
                    }

                    final long routeId = route.correlationId();
                    final long contentLength = headersDecoder.contentLength;
                    final int promiseId = ++maxServerStreamId << 1;

                    doEncodePushPromise(traceId, authorization, pushId, promiseId, promise);

                    final Http2Exchange exchange = new Http2Exchange(routeId, promiseId, contentLength);

                    final HttpBeginExFW beginEx = beginExRW.wrap(extensionBuffer, 0, extensionBuffer.capacity())
                                                   .typeId(httpTypeId)
                                                   .headers(hs -> headers.forEach((n, v) -> hs.item(i -> i.name(n).value(v))))
                                                   .build();

                    exchange.doRequestBegin(traceId, authorization, beginEx);
                    correlations.put(exchange.responseId, exchange);

                    exchange.doRequestEnd(traceId, authorization, EMPTY_OCTETS);
                }
                else
                {
                    counters.pushPromiseFramesSkipped.getAsLong();
                }
            }
        }

        private void doEncodeSettings(
            long traceId,
            long authorization)
        {
            final Http2SettingsFW http2Settings = http2SettingsRW.wrap(frameBuffer, 0, frameBuffer.capacity())
                                                                 .streamId(0)
                                                                 .maxConcurrentStreams(initialSettings.maxConcurrentStreams)
                                                                 .initialWindowSize(initialSettings.initialWindowSize)
                                                                 .maxHeaderListSize(initialSettings.maxHeaderListSize)
                                                                 .build();

            doNetworkReservedData(traceId, authorization, 0L, http2Settings);

            counters.settingsFramesWritten.getAsLong();
        }

        private void doEncodeSettingsAck(
            long traceId,
            long authorization)
        {
            final Http2SettingsFW http2Settings = http2SettingsRW.wrap(frameBuffer, 0, frameBuffer.capacity())
                                                                 .streamId(0)
                                                                 .ack()
                                                                 .build();

            doNetworkReservedData(traceId, authorization, 0L, http2Settings);

            counters.settingsFramesWritten.getAsLong();
        }

        private void doEncodeGoaway(
            long traceId,
            long authorization)
        {
            final Http2GoawayFW http2Goaway = http2GoawayRW.wrap(frameBuffer, 0, frameBuffer.capacity())
                                                           .streamId(0)
                                                           .lastStreamId(0) // TODO: maxClientStreamId?
                                                           .errorCode(decodeError)
                                                           .build();

            doNetworkReservedData(traceId, authorization, 0L, http2Goaway);
            doNetworkEnd(traceId, authorization);

            counters.goawayFramesWritten.getAsLong();
        }

        private void doEncodePingAck(
            long traceId,
            long authorization,
            DirectBuffer payload)
        {
            final Http2PingFW http2Ping = http2PingRW.wrap(frameBuffer, 0, frameBuffer.capacity())
                                                     .streamId(0)
                                                     .ack()
                                                     .payload(payload)
                                                     .build();

            doNetworkReservedData(traceId, authorization, 0L, http2Ping);

            counters.pingFramesWritten.getAsLong();
        }

        private void doEncodeHeaders(
            long traceId,
            long authorization,
            int streamId,
            Array32FW<HttpHeaderFW> headers,
            boolean endResponse)
        {
            final Http2HeadersFW http2Headers = http2HeadersRW.wrap(frameBuffer, 0, frameBuffer.capacity())
                                                  .streamId(streamId)
                                                  .headers(hb -> headersEncoder.encodeHeaders(encodeContext, headers, hb))
                                                  .endHeaders()
                                                  .endStream(endResponse)
                                                  .build();

            doNetworkHeadersData(traceId, authorization, 0L, http2Headers);

            counters.headersFramesWritten.getAsLong();
        }

        private void doEncodeData(
            long traceId,
            long authorization,
            int flags,
            long budgetId,
            int reserved,
            int streamId,
            OctetsFW payload)
        {
            final DirectBuffer buffer = payload.buffer();
            final int offset = payload.offset();
            final int limit = payload.limit();

            int frameOffset = 0;
            int progress = offset;
            while (progress < limit)
            {
                final int length = Math.min(limit - progress, remoteSettings.maxFrameSize);
                final Http2DataFW http2Data = http2DataRW.wrap(frameBuffer, frameOffset, frameBuffer.capacity())
                                                         .streamId(streamId)
                                                         .payload(buffer, progress, length)
                                                         .build();
                frameOffset = http2Data.limit();
                progress += length;

                counters.dataFramesWritten.getAsLong();
            }

            assert progress == limit;

            doNetworkData(traceId, authorization, 0L, reserved, frameBuffer, 0, frameOffset);
        }

        private void doEncodeTrailers(
            long traceId,
            long authorization,
            int streamId,
            Array32FW<HttpHeaderFW> trailers)
        {
            if (trailers.isEmpty())
            {
                final Http2DataFW http2Data = http2DataRW.wrap(frameBuffer, 0, frameBuffer.capacity())
                                                         .streamId(streamId)
                                                         .endStream()
                                                         .build();

                doNetworkReservedData(traceId, authorization, 0L, http2Data);
            }
            else
            {
                final Http2HeadersFW http2Headers = http2HeadersRW.wrap(frameBuffer, 0, frameBuffer.capacity())
                                                      .streamId(streamId)
                                                      .headers(hb -> headersEncoder.encodeTrailers(encodeContext, trailers, hb))
                                                      .endHeaders()
                                                      .endStream()
                                                      .build();

                doNetworkReservedData(traceId, authorization, 0L, http2Headers);
            }

            counters.headersFramesWritten.getAsLong();
        }

        private void doEncodePushPromise(
            long traceId,
            long authorization,
            int streamId,
            int promiseId,
            Array32FW<HttpHeaderFW> promise)
        {
            final Http2PushPromiseFW http2PushPromise = http2PushPromiseRW.wrap(frameBuffer, 0, frameBuffer.capacity())
                                                          .streamId(streamId)
                                                          .promisedStreamId(promiseId)
                                                          .headers(hb -> headersEncoder.encodePromise(encodeContext, promise, hb))
                                                          .endHeaders()
                                                          .build();

            doNetworkHeadersData(traceId, authorization, 0L, http2PushPromise);

            counters.pushPromiseFramesWritten.getAsLong();
        }

        private void doEncodeRstStream(
            long traceId,
            long authorization,
            int streamId,
            Http2ErrorCode error)
        {
            final Http2RstStreamFW http2RstStream = http2RstStreamRW.wrap(frameBuffer, 0, frameBuffer.capacity())
                                                                    .streamId(streamId)
                                                                    .errorCode(error)
                                                                    .build();

            doNetworkReservedData(traceId, authorization, 0L, http2RstStream);

            counters.resetStreamFramesWritten.getAsLong();
        }

        private void doEncodeWindowUpdates(
            long traceId,
            long authorization,
            int streamId,
            int size)
        {
            final int frameOffset = http2WindowUpdateRW.wrap(frameBuffer, 0, frameBuffer.capacity())
                                                       .streamId(0)
                                                       .size(size)
                                                       .build()
                                                       .limit();

            final int frameLimit = http2WindowUpdateRW.wrap(frameBuffer, frameOffset, frameBuffer.capacity())
                                                      .streamId(streamId)
                                                      .size(size)
                                                      .build()
                                                      .limit();

            doNetworkReservedData(traceId, authorization, 0L, frameBuffer, 0, frameLimit);

            counters.windowUpdateFramesWritten.getAsLong();
        }

        private void cleanupNetwork(
            long traceId,
            long authorization)
        {
            cleanup(traceId, authorization, this::doNetworkResetAndAbort);
        }

        private void doNetworkResetAndAbort(
            long traceId,
            long authorization)
        {
            doNetworkReset(traceId, authorization);
            doNetworkAbort(traceId, authorization);
        }

        private void cleanupDecodeSlotIfNecessary()
        {
            if (decodeSlot != NO_SLOT)
            {
                bufferPool.release(decodeSlot);
                decodeSlot = NO_SLOT;
                decodeSlotOffset = 0;
                decodeSlotReserved = 0;
            }
        }

        private void cleanupHeadersSlotIfNecessary()
        {
            if (headersSlot != NO_SLOT)
            {
                bufferPool.release(headersSlot);
                headersSlot = NO_SLOT;
                headersSlotOffset = 0;
            }
        }

        private void cleanupEncodeSlotIfNecessary()
        {
            if (encodeSlot != NO_SLOT)
            {
                bufferPool.release(encodeSlot);
                encodeSlot = NO_SLOT;
                encodeSlotOffset = 0;

                if (Http2Configuration.DEBUG_HTTP2_BUDGETS)
                {
                    System.out.format("[%d] [cleanupEncodeSlotIfNecessary] [0x%016x] [0x%016x] encode encodeSlotOffset => %d\n",
                        System.nanoTime(), 0, budgetId, encodeSlotOffset);
                }
            }
        }

        private void cleanupBudgetCreditorIfNecessary()
        {
            if (responseSharedBudgetIndex != NO_CREDITOR_INDEX)
            {
                creditor.release(responseSharedBudgetIndex);
                responseSharedBudgetIndex = NO_CREDITOR_INDEX;
            }
        }

        private final class Http2Exchange
        {
            private final MessageConsumer application;
            private final long routeId;
            private final long requestId;
            private final long responseId;
            private final int streamId;
            private final long contentLength;

            private int state;
            private long contentObserved;

            private long requestBudgetId;
            private int requestBudget;
            private int requestPadding;
            private BudgetDebitor requestDebitor;
            private long requestDebitorIndex = NO_DEBITOR_INDEX;

            private int responseBudget;

            private int localBudget;
            private int remoteBudget;

            private int requestSlot = NO_SLOT;
            private int requestSlotOffset;
            private long requestSlotTraceId;
            private long requestSlotAuthorization;

            private Http2Exchange(
                long routeId,
                int streamId,
                long contentLength)
            {
                this.routeId = routeId;
                this.streamId = streamId;
                this.contentLength = contentLength;
                this.requestId = supplyInitialId.applyAsLong(routeId);
                this.application = router.supplyReceiver(requestId);
                this.responseId = supplyReplyId.applyAsLong(requestId);
            }

            private void doRequestBegin(
                long traceId,
                long authorization,
                Flyweight extension)
            {
                assert state == 0;
                state = Http2State.openingInitial(state);

                doBegin(application, routeId, requestId, traceId, authorization, affinity, extension);
                router.setThrottle(requestId, this::onRequest);
                streams.put(streamId, this);
                streamsActive[streamId & 0x01]++;
                applicationHeadersProcessed.add(streamId);
                localBudget = localSettings.initialWindowSize;

                onResponseWindowUpdate(traceId, authorization, remoteSettings.initialWindowSize);
            }

            private void doRequestData(
                long traceId,
                long authorization,
                DirectBuffer buffer,
                int offset,
                int limit)
            {
                assert Http2State.initialOpening(state);

                final int length = limit - offset;

                localBudget -= length;

                if (localBudget < 0)
                {
                    doEncodeRstStream(traceId, authorization, streamId, Http2ErrorCode.FLOW_CONTROL_ERROR);
                    cleanup(traceId, authorization);
                }
                else
                {
                    if (requestSlot != NO_SLOT)
                    {
                        final MutableDirectBuffer requestBuffer = bufferPool.buffer(requestSlot);
                        requestBuffer.putBytes(requestSlotOffset, buffer, offset, length);
                        requestSlotOffset += length;
                        requestSlotTraceId = traceId;
                        requestSlotAuthorization = authorization;

                        buffer = requestBuffer;
                        offset = 0;
                        limit = requestSlotOffset;
                    }

                    flushRequestData(traceId, authorization, buffer, offset, limit);

                    contentObserved += length;
                }
            }

            private void doRequestEnd(
                long traceId,
                long authorization,
                Flyweight extension)
            {
                if (!Http2State.initialOpened(state) || requestSlot != NO_SLOT)
                {
                    state = Http2State.closingInitial(state);
                }
                else
                {
                    flushRequestEnd(traceId, authorization, extension);
                }
            }

            private void doRequestAbort(
                long traceId,
                long authorization,
                Flyweight extension)
            {
                setRequestClosed();

                doAbort(application, routeId, requestId, traceId, authorization, extension);
            }

            private void doRequestAbortIfNecessary(
                long traceId,
                long authorization)
            {
                if (!Http2State.initialClosed(state))
                {
                    doRequestAbort(traceId, authorization, EMPTY_OCTETS);
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
                final boolean correlated = correlations.remove(responseId) == null;

                setRequestClosed();

                final long traceId = reset.traceId();
                final long authorization = reset.authorization();

                if (correlated)
                {
                    doEncodeRstStream(traceId, authorization, streamId, Http2ErrorCode.NO_ERROR);
                }
                else
                {
                    doEncodeHeaders(traceId, authorization, streamId, HEADERS_404_NOT_FOUND, true);
                }

                decodeNetworkIfNecessary(traceId, authorization);
                cleanup(traceId, authorization);
            }

            private void onRequestWindow(
                WindowFW window)
            {
                final long traceId = window.traceId();
                final long authorization = window.authorization();
                final long budgetId = window.budgetId();
                final int credit = window.credit();
                final int padding = window.padding();

                state = Http2State.openInitial(state);

                requestBudgetId = budgetId;
                requestBudget += credit;
                requestPadding = padding;

                if (requestBudgetId != 0L && requestDebitorIndex == NO_DEBITOR_INDEX)
                {
                    requestDebitor = supplyDebitor.apply(budgetId);
                    requestDebitorIndex = requestDebitor.acquire(budgetId, initialId, this::flushRequestDataIfNecessary);
                }

                flushRequestDataIfNecessary(traceId);

                if (requestSlot == NO_SLOT)
                {
                    if (!Http2State.initialClosed(state))
                    {
                        if (Http2State.initialClosing(state))
                        {
                            // TODO: trailers extension?
                            flushRequestEnd(traceId, authorization, EMPTY_OCTETS);
                        }
                        else
                        {
                            flushRequestWindowUpdate(traceId, authorization);
                        }
                    }
                }

                applicationHeadersProcessed.remove(streamId);
                decodeNetworkIfNecessary(traceId, authorization);
            }

            private void flushRequestDataIfNecessary(
                long traceId)
            {
                if (requestSlot != NO_SLOT)
                {
                    final MutableDirectBuffer buffer = bufferPool.buffer(requestSlot);
                    final int offset = 0;
                    final int limit = requestSlotOffset;

                    flushRequestData(requestSlotTraceId, requestSlotAuthorization, buffer, offset, limit);
                }
            }

            private void flushRequestData(
                long traceId,
                long authorization,
                DirectBuffer buffer,
                int offset,
                int limit)
            {
                final int maxLength = limit - offset;

                int length = Math.max(Math.min(requestBudget - requestPadding, maxLength), 0);
                int reserved = length + requestPadding;

                if (requestDebitorIndex != NO_DEBITOR_INDEX && requestDebitor != null)
                {
                    final int minimum = reserved; // TODO: fragmentation
                    reserved = requestDebitor.claim(requestDebitorIndex, requestId, minimum, reserved);
                    length = Math.max(reserved - requestPadding, 0);
                }

                if (length > 0)
                {

                    requestBudget -= reserved;

                    assert requestBudget >= 0;

                    doData(application, routeId, requestId, traceId, authorization, requestBudgetId,
                        reserved, buffer, offset, length, EMPTY_OCTETS);
                }

                final int remaining = maxLength - length;
                if (remaining > 0)
                {
                    if (requestSlot == NO_SLOT)
                    {
                        requestSlot = bufferPool.acquire(requestId);
                    }

                    if (requestSlot == NO_SLOT)
                    {
                        doEncodeRstStream(traceId, authorization, streamId, Http2ErrorCode.INTERNAL_ERROR);
                        cleanup(traceId, authorization);
                    }
                    else
                    {
                        final MutableDirectBuffer requestBuffer = bufferPool.buffer(requestSlot);
                        requestBuffer.putBytes(0, buffer, offset, remaining);
                        requestSlotOffset = remaining;
                        requestSlotTraceId = traceId;
                    }
                }
                else
                {
                    cleanupRequestSlotIfNecessary();
                }
            }

            private void flushRequestEnd(
                long traceId,
                long authorization,
                Flyweight extension)
            {
                setRequestClosed();
                doEnd(application, routeId, requestId, traceId, authorization, extension);
            }

            private void flushRequestWindowUpdate(
                long traceId,
                long authorization)
            {
                final int size = requestBudget - Math.max(localBudget, 0);
                if (size > 0)
                {
                    localBudget = requestBudget;
                    doEncodeWindowUpdates(traceId, authorization, streamId, size);
                }
            }

            private void setRequestClosed()
            {
                assert !Http2State.initialClosed(state);

                state = Http2State.closeInitial(state);
                cleanupRequestSlotIfNecessary();
                cleanupRequestDebitorIfNecessary();
                removeStreamIfNecessary();
            }

            private void cleanupRequestSlotIfNecessary()
            {
                if (requestSlot != NO_SLOT)
                {
                    bufferPool.release(requestSlot);
                    requestSlot = NO_SLOT;
                    requestSlotOffset = 0;
                    requestSlotTraceId = 0L;
                    requestSlotAuthorization = 0L;
                }
            }

            private void cleanupRequestDebitorIfNecessary()
            {
                if (requestDebitorIndex != NO_DEBITOR_INDEX)
                {
                    requestDebitor.release(requestDebitorIndex, initialId);
                    requestDebitorIndex = NO_DEBITOR_INDEX;
                }
            }

            private boolean isResponseOpen()
            {
                return Http2State.replyOpened(state);
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
                state = Http2State.openReply(state);

                final HttpBeginExFW beginEx = begin.extension().get(beginExRO::tryWrap);
                final Array32FW<HttpHeaderFW> headers = beginEx != null ? beginEx.headers() : HEADERS_200_OK;

                final long traceId = begin.traceId();
                final long authorization = begin.authorization();

                doEncodeHeaders(traceId, authorization, streamId, headers, false);
            }

            private void onResponseData(
                DataFW data)
            {
                final int reserved = data.reserved();
                final long traceId = data.traceId();

                if (Http2Configuration.DEBUG_HTTP2_BUDGETS)
                {
                    System.out.format("[%d] [onResponseData] [0x%016x] [0x%016x] responseBudget %d - %d => %d\n",
                        System.nanoTime(), traceId, budgetId,
                        responseBudget, reserved, responseBudget - reserved);

                    System.out.format("[%d] [onResponseData] [0x%016x] [0x%016x] responseSharedBudget %d - %d => %d\n",
                        System.nanoTime(), traceId, budgetId,
                        responseSharedBudget, reserved, responseSharedBudget - reserved);
                }

                responseBudget -= reserved;
                responseSharedBudget -= reserved;

                assert responseSharedBudget >= 0;

                if (responseBudget < 0)
                {
                    final long authorization = data.authorization();
                    doResponseReset(traceId, authorization);
                    doNetworkAbort(traceId, authorization);
                }
                else
                {
                    final long authorization = data.authorization();
                    final OctetsFW payload = data.payload();
                    final OctetsFW extension = data.extension();
                    final HttpDataExFW dataEx = extension.get(dataExRO::tryWrap);

                    if (dataEx != null)
                    {
                        final Array32FW<HttpHeaderFW> promise = dataEx.promise();

                        onEncodePromise(traceId, authorization, streamId, promise);
                    }

                    if (payload != null)
                    {
                        final int flags = data.flags();
                        final long budgetId = data.budgetId();
                        final int length = data.length();

                        if (Http2Configuration.DEBUG_HTTP2_BUDGETS)
                        {
                            System.out.format("[%d] [onResponseData] [0x%016x] [0x%016x] remoteBudget %d - %d => %d \n",
                                System.nanoTime(), traceId, budgetId, remoteBudget, length, remoteBudget - length);

                            System.out.format("[%d] [onResponseData] [0x%016x] [0x%016x] remoteSharedBudget %d - %d => %d \n",
                                System.nanoTime(), traceId, budgetId, remoteSharedBudget, length, remoteSharedBudget - length);
                        }

                        remoteBudget -= length;
                        remoteSharedBudget -= length;

                        doEncodeData(traceId, authorization, flags, budgetId, reserved, streamId, payload);

                        final int remotePaddableMax = Math.min(remoteBudget, bufferPool.slotCapacity());
                        final int remotePadding = framePadding(remotePaddableMax, remoteSettings.maxFrameSize);
                        final int responsePadding = replyPadding + remotePadding;

                        final int minimumClaim = 1024;
                        final int responseCreditMin = (responseBudget <= responsePadding + minimumClaim) ? 0 : remoteBudget >> 1;

                        flushResponseWindow(traceId, authorization, responseCreditMin);
                    }
                }
            }

            private void onResponseEnd(
                EndFW end)
            {
                setResponseClosed();

                final HttpEndExFW endEx = end.extension().get(endExRO::tryWrap);
                final Array32FW<HttpHeaderFW> trailers = endEx != null ? endEx.trailers() : TRAILERS_EMPTY;

                final long traceId = end.traceId();
                final long authorization = end.authorization();

                doEncodeTrailers(traceId, authorization, streamId, trailers);
            }

            private void onResponseAbort(
                AbortFW abort)
            {
                setResponseClosed();

                final long traceId = abort.traceId();
                final long authorization = abort.authorization();

                doEncodeRstStream(traceId, authorization, streamId, Http2ErrorCode.NO_ERROR);
                cleanup(traceId, authorization);
            }

            private void doResponseReset(
                long traceId,
                long authorization)
            {
                setResponseClosed();

                doReset(application, routeId, responseId, traceId, authorization);
            }

            private void doResponseResetIfNecessary(
                long traceId,
                long authorization)
            {
                correlations.remove(responseId);

                if (!Http2State.replyClosed(state))
                {
                    doResponseReset(traceId, authorization);
                }
            }

            private void onResponseWindowUpdate(
                long traceId,
                long authorization,
                int size)
            {
                final long newRemoteBudget = (long) remoteBudget + size;

                if (newRemoteBudget > MAX_REMOTE_BUDGET)
                {
                    doEncodeRstStream(traceId, authorization, streamId, Http2ErrorCode.FLOW_CONTROL_ERROR);
                    cleanup(traceId, authorization);
                }
                else
                {
                    remoteBudget = (int) newRemoteBudget;

                    flushResponseWindow(traceId, authorization, 0);
                }
            }

            private void flushResponseWindow(
                long traceId,
                long authorization,
                int responseCreditMin)
            {
                if (!Http2State.replyClosed(state))
                {
                    final int remotePaddableMax = Math.min(remoteBudget, bufferPool.slotCapacity());
                    final int remotePadding = framePadding(remotePaddableMax, remoteSettings.maxFrameSize);
                    final int responsePadding = replyPadding + remotePadding;
                    final int responseBudgetMax = remoteBudget + responsePadding;
                    final int responseCredit = responseBudgetMax - responseBudget;

                    if (responseCredit > 0 && responseCredit >= responseCreditMin)
                    {
                        if (Http2Configuration.DEBUG_HTTP2_BUDGETS)
                        {
                            System.out.format("[%d] [flushResponseWindow] [0x%016x] [0x%016x] responseBudget %d + %d => %d\n",
                                System.nanoTime(), traceId, budgetId,
                                responseBudget, responseCredit, responseBudget + responseCredit);
                        }

                        responseBudget += responseCredit;

                        doWindow(application, routeId, responseId, traceId, authorization,
                            budgetId, responseCredit, responsePadding);
                    }
                }
            }

            private void setResponseClosed()
            {
                assert !Http2State.replyClosed(state);

                state = Http2State.closeReply(state);
                removeStreamIfNecessary();
            }

            private void removeStreamIfNecessary()
            {
                if (Http2State.closed(state))
                {
                    streams.remove(streamId);
                    streamsActive[streamId & 0x01]--;
                }
            }

            private void cleanup(
                long traceId,
                long authorization)
            {
                doRequestAbortIfNecessary(traceId, authorization);
                doResponseResetIfNecessary(traceId, authorization);
            }
        }
    }

    private static final class Http2State
    {
        private static final int INITIAL_OPENING = 0x10;
        private static final int INITIAL_OPENED = 0x20;
        private static final int INITIAL_CLOSING = 0x40;
        private static final int INITIAL_CLOSED = 0x80;
        private static final int REPLY_OPENED = 0x01;
        private static final int REPLY_CLOSING = 0x02;
        private static final int REPLY_CLOSED = 0x04;

        static int openingInitial(
            int state)
        {
            return state | INITIAL_OPENING;
        }

        static int openInitial(
            int state)
        {
            return openingInitial(state) | INITIAL_OPENED;
        }

        static int closingInitial(
            int state)
        {
            return state | INITIAL_CLOSING;
        }

        static int closeInitial(
            int state)
        {
            return closingInitial(state) | INITIAL_CLOSED;
        }

        static boolean initialOpening(
            int state)
        {
            return (state & INITIAL_OPENING) != 0;
        }

        static boolean initialOpened(
            int state)
        {
            return (state & INITIAL_OPENED) != 0;
        }

        static boolean initialClosing(
            int state)
        {
            return (state & INITIAL_CLOSING) != 0;
        }

        static boolean initialClosed(
            int state)
        {
            return (state & INITIAL_CLOSED) != 0;
        }

        static boolean closed(
            int state)
        {
            return initialClosed(state) && replyClosed(state);
        }

        static int openReply(
            int state)
        {
            return state | REPLY_OPENED;
        }

        static boolean replyOpened(
            int state)
        {
            return (state & REPLY_OPENED) != 0;
        }

        static int closingReply(
            int state)
        {
            return state | REPLY_CLOSING;
        }

        static boolean replyClosing(
            int state)
        {
            return (state & REPLY_CLOSING) != 0;
        }

        static int closeReply(
            int state)
        {
            return closingReply(state) | REPLY_CLOSED;
        }

        static boolean replyClosed(
            int state)
        {
            return (state & REPLY_CLOSED) != 0;
        }
    }

    private final class Http2HeadersDecoder
    {
        private HpackContext context;
        private int headerTableSize;
        private boolean pseudoHeaders;
        private MutableBoolean expectDynamicTableSizeUpdate;

        private final Consumer<HpackHeaderFieldFW> decodeHeader;
        private final Consumer<HpackHeaderFieldFW> decodeTrailer;
        private int method;
        private int scheme;
        private int path;

        Http2ErrorCode connectionError;
        Http2ErrorCode streamError;
        Array32FW<HttpHeaderFW> httpErrorHeader;

        final Map<String, String> headers = new LinkedHashMap<>();
        long contentLength = -1;

        private Http2HeadersDecoder()
        {
            BiConsumer<DirectBuffer, DirectBuffer> nameValue =
                ((BiConsumer<DirectBuffer, DirectBuffer>) this::collectHeaders)
                    .andThen(this::validatePseudoHeaders)
                    .andThen(this::uppercaseHeaders)
                    .andThen(this::connectionHeaders)
                    .andThen(this::contentLengthHeader)
                    .andThen(this::teHeader);

            Consumer<HpackHeaderFieldFW> consumer = this::validateHeaderFieldType;
            consumer = consumer.andThen(this::dynamicTableSizeUpdate);
            this.decodeHeader = consumer.andThen(h -> decodeHeaderField(h, nameValue));
            this.decodeTrailer = h -> decodeHeaderField(h, this::validateTrailerFieldName);
        }

        void decodeHeaders(
            HpackContext context,
            int headerTableSize,
            MutableBoolean expectDynamicTableSizeUpdate,
            HpackHeaderBlockFW headerBlock)
        {
            reset(context, headerTableSize, expectDynamicTableSizeUpdate);
            headerBlock.forEach(decodeHeader);

            // All HTTP/2 requests MUST include exactly one valid value for the
            // ":method", ":scheme", and ":path" pseudo-header fields, unless it is
            // a CONNECT request (Section 8.3).  An HTTP request that omits
            // mandatory pseudo-header fields is malformed
            if (!error() && (method != 1 || scheme != 1 || path != 1))
            {
                streamError = Http2ErrorCode.PROTOCOL_ERROR;
            }
        }

        void decodeTrailers(
            HpackContext context,
            int headerTableSize,
            MutableBoolean expectDynamicTableSizeUpdate,
            HpackHeaderBlockFW headerBlock)
        {
            reset(context, headerTableSize, expectDynamicTableSizeUpdate);
            headerBlock.forEach(decodeTrailer);
        }

        boolean error()
        {
            return streamError != null || connectionError != null;
        }

        boolean httpError()
        {
            return httpErrorHeader != null;
        }

        private void reset(
            HpackContext context,
            int headerTableSize,
            MutableBoolean expectDynamicTableSizeUpdate)
        {
            this.context = context;
            this.headerTableSize = headerTableSize;
            this.expectDynamicTableSizeUpdate = expectDynamicTableSizeUpdate;
            this.headers.clear();
            this.connectionError = null;
            this.streamError = null;
            this.httpErrorHeader = null;
            this.pseudoHeaders = true;
            this.method = 0;
            this.scheme = 0;
            this.path = 0;
            this.contentLength = -1;
        }

        private void validateHeaderFieldType(
            HpackHeaderFieldFW hf)
        {
            if (!error() && hf.type() == UNKNOWN)
            {
                connectionError = Http2ErrorCode.COMPRESSION_ERROR;
            }
        }

        private void dynamicTableSizeUpdate(
            HpackHeaderFieldFW hf)
        {
            if (!error())
            {
                switch (hf.type())
                {
                case INDEXED:
                case LITERAL:
                    expectDynamicTableSizeUpdate.value = false;
                    break;
                case UPDATE:
                    if (!expectDynamicTableSizeUpdate.value)
                    {
                        // dynamic table size update MUST occur at the beginning of the first header block
                        connectionError = Http2ErrorCode.COMPRESSION_ERROR;
                        return;
                    }
                    int maxTableSize = hf.tableSize();
                    if (maxTableSize > headerTableSize)
                    {
                        connectionError = Http2ErrorCode.COMPRESSION_ERROR;
                        return;
                    }
                    context.updateSize(hf.tableSize());
                    break;
                default:
                    break;
                }
            }
        }

        private void validatePseudoHeaders(
            DirectBuffer name,
            DirectBuffer value)
        {
            if (!error())
            {
                if (name.capacity() > 0 && name.getByte(0) == ':')
                {
                    // All pseudo-header fields MUST appear in the header block before regular header fields
                    if (!pseudoHeaders)
                    {
                        streamError = Http2ErrorCode.PROTOCOL_ERROR;
                        return;
                    }
                    // request pseudo-header fields MUST be one of :authority, :method, :path, :scheme,
                    int index = context.index(name);
                    switch (index)
                    {
                    case 1:             // :authority
                        break;
                    case 2:             // :method
                        method++;
                        break;
                    case 4:             // :path
                        if (value.capacity() > 0)       // :path MUST not be empty
                        {
                            path++;
                            if (!HttpUtil.isPathValid(value))
                            {
                                httpErrorHeader = HEADERS_400_BAD_REQUEST;
                            }
                        }
                        break;
                    case 6:             // :scheme
                        scheme++;
                        break;
                    default:
                        streamError = Http2ErrorCode.PROTOCOL_ERROR;
                        return;
                    }
                }
                else
                {
                    pseudoHeaders = false;
                }
            }
        }

        private void validateTrailerFieldName(
            DirectBuffer name,
            DirectBuffer value)
        {
            if (!error())
            {
                if (name.capacity() > 0 && name.getByte(0) == ':')
                {
                    streamError = Http2ErrorCode.PROTOCOL_ERROR;
                    return;
                }
            }
        }

        private void connectionHeaders(
            DirectBuffer name,
            DirectBuffer value)
        {
            if (!error() && name.equals(HpackContext.CONNECTION))
            {
                streamError = Http2ErrorCode.PROTOCOL_ERROR;
            }
        }

        private void contentLengthHeader(
            DirectBuffer name,
            DirectBuffer value)
        {
            if (!error() && name.equals(context.nameBuffer(28)))
            {
                String contentLength = value.getStringWithoutLengthUtf8(0, value.capacity());
                this.contentLength = Long.parseLong(contentLength);
            }
        }

        // 8.1.2.2 TE header MUST NOT contain any value other than "trailers".
        private void teHeader(
            DirectBuffer name,
            DirectBuffer value)
        {
            if (!error() && name.equals(TE) && !value.equals(TRAILERS))
            {
                streamError = Http2ErrorCode.PROTOCOL_ERROR;
            }
        }

        private void uppercaseHeaders(
            DirectBuffer name,
            DirectBuffer value)
        {
            if (!error())
            {
                for (int i = 0; i < name.capacity(); i++)
                {
                    if (name.getByte(i) >= 'A' && name.getByte(i) <= 'Z')
                    {
                        streamError = Http2ErrorCode.PROTOCOL_ERROR;
                    }
                }
            }
        }

        // Collect headers into map to resolve target
        // TODO avoid this
        private void collectHeaders(
            DirectBuffer name,
            DirectBuffer value)
        {
            if (!error())
            {
                String nameStr = name.getStringWithoutLengthUtf8(0, name.capacity());
                String valueStr = value.getStringWithoutLengthUtf8(0, value.capacity());
                // TODO cookie needs to be appended with ';'
                headers.merge(nameStr, valueStr, (o, n) -> String.format("%s, %s", o, n));
            }
        }

        private void decodeHeaderField(
            HpackHeaderFieldFW hf,
            BiConsumer<DirectBuffer, DirectBuffer> nameValue)
        {
            if (!error())
            {
                decodeHF(hf, nameValue);
            }
        }

        private void decodeHF(
            HpackHeaderFieldFW hf,
            BiConsumer<DirectBuffer, DirectBuffer> nameValue)
        {
            int index;
            DirectBuffer name = null;
            DirectBuffer value = null;

            switch (hf.type())
            {
            case INDEXED :
                index = hf.index();
                if (!context.valid(index))
                {
                    connectionError = Http2ErrorCode.COMPRESSION_ERROR;
                    return;
                }
                name = context.nameBuffer(index);
                value = context.valueBuffer(index);
                nameValue.accept(name, value);
                break;

            case LITERAL :
                HpackLiteralHeaderFieldFW hpackLiteral = hf.literal();
                if (hpackLiteral.error())
                {
                    connectionError = Http2ErrorCode.COMPRESSION_ERROR;
                    return;
                }

                HpackStringFW hpackValue = hpackLiteral.valueLiteral();

                switch (hpackLiteral.nameType())
                {
                case INDEXED:
                    index = hpackLiteral.nameIndex();
                    if (!context.valid(index))
                    {
                        connectionError = Http2ErrorCode.COMPRESSION_ERROR;
                        return;
                    }
                    name = context.nameBuffer(index);

                    value = hpackValue.payload();
                    if (hpackValue.huffman())
                    {
                        MutableDirectBuffer dst = new UnsafeBuffer(new byte[4096]); // TODO
                        int length = HpackHuffman.decode(value, dst);
                        if (length == -1)
                        {
                            connectionError = Http2ErrorCode.COMPRESSION_ERROR;
                            return;
                        }
                        value = new UnsafeBuffer(dst, 0, length);
                    }
                    nameValue.accept(name, value);
                    break;
                case NEW:
                    HpackStringFW hpackName = hpackLiteral.nameLiteral();
                    name = hpackName.payload();
                    if (hpackName.huffman())
                    {
                        MutableDirectBuffer dst = new UnsafeBuffer(new byte[4096]); // TODO
                        int length = HpackHuffman.decode(name, dst);
                        if (length == -1)
                        {
                            connectionError = Http2ErrorCode.COMPRESSION_ERROR;
                            return;
                        }
                        name = new UnsafeBuffer(dst, 0, length);
                    }

                    value = hpackValue.payload();
                    if (hpackValue.huffman())
                    {
                        MutableDirectBuffer dst = new UnsafeBuffer(new byte[4096]); // TODO
                        int length = HpackHuffman.decode(value, dst);
                        if (length == -1)
                        {
                            connectionError = Http2ErrorCode.COMPRESSION_ERROR;
                            return;
                        }
                        value = new UnsafeBuffer(dst, 0, length);
                    }
                    nameValue.accept(name, value);
                    break;
                }
                if (hpackLiteral.literalType() == INCREMENTAL_INDEXING)
                {
                    // make a copy for name and value as they go into dynamic table (outlives current frame)
                    MutableDirectBuffer nameCopy = new UnsafeBuffer(new byte[name.capacity()]);
                    nameCopy.putBytes(0, name, 0, name.capacity());
                    MutableDirectBuffer valueCopy = new UnsafeBuffer(new byte[value.capacity()]);
                    valueCopy.putBytes(0, value, 0, value.capacity());
                    context.add(nameCopy, valueCopy);
                }
                break;
            default:
                break;
            }
        }
    }

    private final class Http2HeadersEncoder
    {
        private HpackContext context;

        private boolean status;
        private boolean accessControlAllowOrigin;
        private boolean serverHeader;
        private final List<String> connectionHeaders = new ArrayList<>();

        private final Consumer<HttpHeaderFW> search = ((Consumer<HttpHeaderFW>) this::status)
            .andThen(this::accessControlAllowOrigin)
            .andThen(this::serverHeader)
            .andThen(this::connectionHeaders);

        void encodePromise(
            HpackContext encodeContext,
            Array32FW<HttpHeaderFW> headers,
            HpackHeaderBlockFW.Builder headerBlock)
        {
            reset(encodeContext);
            headers.forEach(h -> headerBlock.header(b -> encodeHeader(h, b)));
        }

        void encodeHeaders(
            HpackContext encodeContext,
            Array32FW<HttpHeaderFW> headers,
            HpackHeaderBlockFW.Builder headerBlock)
        {
            reset(encodeContext);

            headers.forEach(search);

            if (!status)
            {
                headerBlock.header(b -> b.indexed(8));
            }

            headers.forEach(h ->
            {
                if (includeHeader(h))
                {
                    headerBlock.header(b -> encodeHeader(h, b));
                }
            });

            if (config.accessControlAllowOrigin() && !accessControlAllowOrigin)
            {
                headerBlock.header(b -> b.literal(l -> l.type(WITHOUT_INDEXING)
                                                        .name(20)
                                                        .value(DEFAULT_ACCESS_CONTROL_ALLOW_ORIGIN)));
            }

            // add configured Server header if there is no Server header in response
            if (config.serverHeader() != null && !serverHeader)
            {
                DirectBuffer server = config.serverHeader();
                headerBlock.header(b -> b.literal(l -> l.type(WITHOUT_INDEXING).name(54).value(server)));
            }
        }

        void encodeTrailers(
            HpackContext encodeContext,
            Array32FW<HttpHeaderFW> headers,
            HpackHeaderBlockFW.Builder headerBlock)
        {
            reset(encodeContext);
            headers.forEach(h -> headerBlock.header(b -> encodeHeader(h, b)));
        }

        private void reset(HpackContext encodeContext)
        {
            context = encodeContext;
            status = false;
            accessControlAllowOrigin = false;
            serverHeader = false;
            connectionHeaders.clear();
        }

        private void status(
            HttpHeaderFW header)
        {
            status |= header.name().value().equals(context.nameBuffer(8));
        }

        private void accessControlAllowOrigin(
            HttpHeaderFW header)
        {
            accessControlAllowOrigin |= header.name().value().equals(context.nameBuffer(20));
        }

        // Checks if response has server header
        private void serverHeader(
            HttpHeaderFW header)
        {
            serverHeader |= header.name().value().equals(context.nameBuffer(54));
        }

        private void connectionHeaders(
            HttpHeaderFW header)
        {
            final String8FW name = header.name();

            if (name.value().equals(CONNECTION))
            {
                final String16FW value = header.value();
                final String[] headerValues = value.asString().split(",");
                for (String headerValue : headerValues)
                {
                    connectionHeaders.add(headerValue.trim());
                }
            }
        }

        private boolean includeHeader(
            HttpHeaderFW header)
        {
            final String8FW name = header.name();
            final DirectBuffer nameBuffer = name.value();

            // Excluding 8.1.2.1 pseudo-header fields
            if (nameBuffer.equals(context.nameBuffer(1)) ||      // :authority
                nameBuffer.equals(context.nameBuffer(2)) ||      // :method
                nameBuffer.equals(context.nameBuffer(4)) ||      // :path
                nameBuffer.equals(context.nameBuffer(6)))        // :scheme
            {
                return false;
            }

            // Excluding 8.1.2.2 connection-specific header fields
            if (nameBuffer.equals(context.nameBuffer(57)) ||     // transfer-encoding
                nameBuffer.equals(CONNECTION) ||                 // connection
                nameBuffer.equals(KEEP_ALIVE) ||                 // keep-alive
                nameBuffer.equals(PROXY_CONNECTION) ||           // proxy-connection
                nameBuffer.equals(UPGRADE))                      // upgrade
            {
                return false;
            }

            // Excluding header if nominated by connection header field
            if (connectionHeaders.contains(name.asString()))
            {
                return false;
            }

            return true;
        }

        private void encodeHeader(
            HttpHeaderFW header,
            HpackHeaderFieldFW.Builder builder)
        {
            final String8FW name = header.name();
            final String16FW value = header.value();

            final int index = context.index(name.value(), value.value());
            if (index != -1)
            {
                builder.indexed(index);
            }
            else
            {
                builder.literal(literal -> encodeLiteral(literal, context, name.value(), value.value()));
            }
        }

        // TODO dynamic table, Huffman, never indexed
        private void encodeLiteral(
            HpackLiteralHeaderFieldFW.Builder builder,
            HpackContext hpackContext,
            DirectBuffer nameBuffer,
            DirectBuffer valueBuffer)
        {
            builder.type(WITHOUT_INDEXING);
            final int nameIndex = hpackContext.index(nameBuffer);
            if (nameIndex != -1)
            {
                builder.name(nameIndex);
            }
            else
            {
                builder.name(nameBuffer, 0, nameBuffer.capacity());
            }
            builder.value(valueBuffer, 0, valueBuffer.capacity());
        }
    }
}
