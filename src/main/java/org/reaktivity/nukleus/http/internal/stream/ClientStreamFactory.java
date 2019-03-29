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

import static java.util.Objects.requireNonNull;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.http.internal.HttpConfiguration;
import org.reaktivity.nukleus.http.internal.types.OctetsFW;
import org.reaktivity.nukleus.http.internal.types.control.HttpRouteExFW;
import org.reaktivity.nukleus.http.internal.types.control.RouteFW;
import org.reaktivity.nukleus.http.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http.internal.types.stream.FrameFW;
import org.reaktivity.nukleus.http.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

public final class ClientStreamFactory implements StreamFactory
{
    static final Map<String, String> EMPTY_HEADERS = Collections.emptyMap();
    static final byte[] CRLFCRLF_BYTES = "\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
    static final byte[] CRLF_BYTES = "\r\n".getBytes(StandardCharsets.US_ASCII);
    static final byte[] SEMICOLON_BYTES = ";".getBytes(StandardCharsets.US_ASCII);

    // Pseudo-headers
    static final int METHOD = 0;
    static final int SCHEME = 1;
    static final int AUTHORITY = 2;
    static final int PATH = 3;

    final FrameFW frameRO = new FrameFW();
    final RouteFW routeRO = new RouteFW();
    private HttpRouteExFW routeExRO = new HttpRouteExFW();

    final BeginFW beginRO = new BeginFW();
    final HttpBeginExFW beginExRO = new HttpBeginExFW();

    final DataFW dataRO = new DataFW();
    final EndFW endRO = new EndFW();
    final AbortFW abortRO = new AbortFW();

    final WindowFW windowRO = new WindowFW();
    final ResetFW resetRO = new ResetFW();

    final RouteManager router;
    final LongUnaryOperator supplyInitialId;
    final LongUnaryOperator supplyReplyId;
    final LongSupplier enqueues;
    final LongSupplier dequeues;
    final LongSupplier supplyTrace;
    final BufferPool bufferPool;
    final MessageWriter writer;
    long supplyTraceId;

    final int maximumHeadersSize;

    Long2ObjectHashMap<Correlation<?>> correlations;

    final Long2ObjectHashMap<ConnectionPool> connectionPools;
    final int maximumConnectionsPerRoute;
    final int maximumQueuedRequestsPerRoute;


    final UnsafeBuffer temporarySlot;
    final LongSupplier countRequests;
    final LongSupplier countRequestsRejected;
    final LongSupplier countRequestsAbandoned;
    final LongSupplier countResponses;
    final LongSupplier countResponsesAbandoned;

    public ClientStreamFactory(
        HttpConfiguration configuration,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        LongUnaryOperator supplyInitialId,
        LongUnaryOperator supplyReplyId,
        Long2ObjectHashMap<Correlation<?>> correlations,
        Function<String, LongSupplier> supplyCounter,
        LongSupplier supplyTrace)
    {
        this.supplyTrace = requireNonNull(supplyTrace);
        this.router = requireNonNull(router);
        this.writer = new MessageWriter(requireNonNull(writeBuffer));
        this.bufferPool = requireNonNull(bufferPool);
        this.supplyInitialId = requireNonNull(supplyInitialId);
        this.supplyReplyId = requireNonNull(supplyReplyId);
        this.correlations = requireNonNull(correlations);
        this.connectionPools = new Long2ObjectHashMap<>();
        this.maximumConnectionsPerRoute = configuration.maximumConnectionsPerRoute();
        this.maximumQueuedRequestsPerRoute = configuration.maximumRequestsQueuedPerRoute();
        this.maximumHeadersSize = bufferPool.slotCapacity();
        this.temporarySlot = new UnsafeBuffer(ByteBuffer.allocateDirect(bufferPool.slotCapacity()));
        this.countRequests = supplyCounter.apply("http.requests");
        this.countRequestsRejected = supplyCounter.apply("http.requests.rejected");
        this.countRequestsAbandoned = supplyCounter.apply("http.requests.abandoned");
        this.countResponses = supplyCounter.apply("http.responses");
        this.countResponsesAbandoned = supplyCounter.apply("http.responses.abandoned");
        this.enqueues = supplyCounter.apply("http.enqueues");
        this.dequeues = supplyCounter.apply("http.dequeues");
    }

    @Override
    public MessageConsumer newStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length,
        MessageConsumer throttle)
    {
        final BeginFW begin = beginRO.wrap(buffer, index, index + length);
        final long streamId = begin.streamId();
        this.supplyTraceId = begin.trace();

        MessageConsumer newStream;

        if ((streamId & 0x0000_0000_0000_0001L) != 0L)
        {
            newStream = newAcceptStream(begin, throttle);
        }
        else
        {
            newStream = newConnectReplyStream(begin, throttle);
        }

        return newStream;
    }

    private MessageConsumer newAcceptStream(
        BeginFW begin,
        MessageConsumer acceptReply)
    {
        final long routeId = begin.routeId();
        final long authorization = begin.authorization();

        final OctetsFW extension = begin.extension();

        // TODO: avoid object creation
        Map<String, String> headers = EMPTY_HEADERS;
        if (extension.sizeof() > 0)
        {
            final HttpBeginExFW beginEx = extension.get(beginExRO::wrap);
            Map<String, String> headers0 = new LinkedHashMap<>();
            beginEx.headers().forEach(h -> headers0.put(h.name().asString(), h.value().asString()));
            headers = headers0;
        }

        final RouteFW route = resolveTarget(routeId, authorization, headers);

        MessageConsumer newStream = null;

        if (route != null)
        {
            final long acceptRouteId = begin.routeId();
            final long acceptId = begin.streamId();
            final long connectRouteId = route.correlationId();
            final long acceptReplyId = supplyReplyId.applyAsLong(acceptId);

            newStream = new ClientAcceptStream(this,
                    acceptReply, acceptRouteId, acceptId, acceptReplyId,
                    connectRouteId, headers);
        }

        return newStream;
    }

    private MessageConsumer newConnectReplyStream(
        BeginFW begin,
        MessageConsumer connectReplyThrottle)
    {
        final long connectRouteId = begin.routeId();
        final long connectReplyId = begin.streamId();

        return new ClientConnectReplyStream(this, connectReplyThrottle, connectRouteId, connectReplyId);
    }

    private RouteFW resolveTarget(
        long routeId,
        long authorization,
        Map<String, String> headers)
    {
        final MessagePredicate filter = (t, b, o, l) ->
        {
            final RouteFW route = routeRO.wrap(b, o, o + l);
            final OctetsFW extension = route.extension();
            boolean headersMatch = true;
            if (extension.sizeof() > 0)
            {
                final HttpRouteExFW routeEx = extension.get(routeExRO::wrap);
                headersMatch = !routeEx.headers().anyMatch(
                        h -> !Objects.equals(h.value().asString(), headers.get(h.name().asString())));
            }
            return headersMatch;
        };

        return router.resolve(routeId, authorization, filter, (msgTypeId, buffer, index, length) ->
            routeRO.wrap(buffer, index, index + length));
    }

}
