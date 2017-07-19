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
package org.reaktivity.nukleus.http.internal.stream;

import static java.util.Objects.requireNonNull;

import java.nio.charset.StandardCharsets;
import java.util.function.LongSupplier;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.http.internal.types.control.RouteFW;
import org.reaktivity.nukleus.http.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.route.RouteHandler;
import org.reaktivity.nukleus.stream.StreamFactory;

public final class ServerStreamFactory implements StreamFactory
{
    static final byte[] CRLFCRLF_BYTES = "\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
    static final byte[] CRLF_BYTES = "\r\n".getBytes(StandardCharsets.US_ASCII);
    static final byte[] SEMICOLON_BYTES = ";".getBytes(StandardCharsets.US_ASCII);
    static final byte[] SPACE = " ".getBytes(StandardCharsets.US_ASCII);
    static final int MAXIMUM_METHOD_BYTES = "OPTIONS".length();

    final MessageWriter writer;

    private final RouteFW routeRO = new RouteFW();
    private final BeginFW beginRO = new BeginFW();

    final RouteHandler router;
    final LongSupplier supplyStreamId;
    final LongSupplier supplyCorrelationId;
    final BufferPool slab;

    Long2ObjectHashMap<Correlation<?>> correlations;

    public ServerStreamFactory(
        Configuration config,
        RouteHandler router,
        MutableDirectBuffer writeBuffer,
        BufferPool bufferPool,
        LongSupplier supplyStreamId,
        LongSupplier supplyCorrelationId,
        Long2ObjectHashMap<Correlation<?>> correlations)
    {
        this.router = requireNonNull(router);
        this.writer = new MessageWriter(requireNonNull(writeBuffer));
        this.slab = requireNonNull(bufferPool);
        this.supplyStreamId = requireNonNull(supplyStreamId);
        this.supplyCorrelationId = supplyCorrelationId;
        this.correlations = requireNonNull(correlations);
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
        final long sourceRef = begin.sourceRef();

        MessageConsumer newStream;

        if (sourceRef == 0L)
        {
            newStream = newConnectReplyStream(begin, throttle);
        }
        else
        {
            newStream = newAcceptStream(begin, throttle);
        }

        return newStream;
    }

    private MessageConsumer newAcceptStream(
        final BeginFW begin,
        final MessageConsumer acceptThrottle)
    {
        final long acceptRef = begin.sourceRef();
        final String acceptName = begin.source().asString();

        final MessagePredicate filter = (t, b, o, l) ->
        {
            final RouteFW route = routeRO.wrap(b, o, l);
            return acceptRef == route.sourceRef() &&
                    acceptName.equals(route.source().asString());
        };

        final RouteFW route = router.resolve(filter, this::wrapRoute);

        MessageConsumer newStream = null;

        if (route != null)
        {
            final long acceptId = begin.streamId();
            final long acceptCorrelationId = begin.correlationId();

            newStream = new ServerAcceptStream(this,
                    acceptThrottle, acceptId, acceptRef, acceptName, acceptCorrelationId);
        }

        return newStream;
    }

    private MessageConsumer newConnectReplyStream(final BeginFW begin, final MessageConsumer connectReplyThrottle)
    {
        final String connectReplyName = begin.source().asString();
        final long connectReplyId = begin.streamId();

        return new ServerConnectReplyStream(this, connectReplyThrottle, connectReplyId,
                connectReplyName);
    }

    private RouteFW wrapRoute(int msgTypeId, DirectBuffer buffer, int index, int length)
    {
        return routeRO.wrap(buffer, index, index + length);
    }

    @FunctionalInterface interface DecoderState
    {
        int decode(DirectBuffer buffer, int offset, int limit);
    }

    static final class HttpStatus
    {
        int status;
        String message;

        void reset()
        {
            status = 200;
            message = "OK";
        }
    }

    enum StandardMethods
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
