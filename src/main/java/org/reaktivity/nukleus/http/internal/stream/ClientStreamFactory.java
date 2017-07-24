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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.MessageHandler;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.http.internal.HttpConfiguration;
import org.reaktivity.nukleus.http.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http.internal.types.stream.FrameFW;
import org.reaktivity.nukleus.http.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.http.internal.util.function.LongObjectBiConsumer;
import org.reaktivity.nukleus.route.RouteHandler;
import org.reaktivity.reaktor.internal.acceptable.Source;
import org.reaktivity.reaktor.internal.buffer.Slab;

public final class ClientStreamFactory
{
    static final Map<String, String> EMPTY_HEADERS = Collections.emptyMap();

    // Pseudo-headers
    static final int METHOD = 0;
    static final int SCHEME = 1;
    static final int AUTHORITY = 2;
    static final int PATH = 3;

    final FrameFW frameRO = new FrameFW();

    final BeginFW beginRO = new BeginFW();
    final HttpBeginExFW beginExRO = new HttpBeginExFW();

    final DataFW dataRO = new DataFW();
    final EndFW endRO = new EndFW();

    final WindowFW windowRO = new WindowFW();
    final ResetFW resetRO = new ResetFW();

    final RouteHandler router;
    final LongSupplier supplyStreamId;
    final LongSupplier supplyCorrelationId;
    final BufferPool slab;
    final MessageWriter writer;

    Long2ObjectHashMap<Correlation<?>> correlations;

    final Map<String, Map<Long, ConnectionPool>> connectionPools;
    final int maximumConnectionsPerRoute;


    public ClientStreamFactory(
        HttpConfiguration configuration,
        RouteHandler router,
        MutableDirectBuffer writeBuffer,
        LongSupplier supplyTargetId,
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
        this.connectionPools = new HashMap<>();
        this.maximumConnectionsPerRoute = configuration.maximumConnectionsPerRoute();
    }

    public MessageHandler newStream()
    {
        return new ClientAcceptStream(this)::handleStream;
    }
}
