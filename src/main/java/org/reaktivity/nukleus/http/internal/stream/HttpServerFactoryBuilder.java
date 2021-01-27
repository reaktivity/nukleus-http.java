/**
 * Copyright 2016-2021 The Reaktivity Project
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

import java.util.function.LongUnaryOperator;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.http.internal.HttpConfiguration;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;
import org.reaktivity.nukleus.stream.StreamFactoryBuilder;

public final class HttpServerFactoryBuilder implements StreamFactoryBuilder
{
    private final HttpConfiguration config;

    private RouteManager router;
    private MutableDirectBuffer writeBuffer;
    private LongUnaryOperator supplyInitialId;
    private LongUnaryOperator supplyReplyId;
    private ToIntFunction<String> supplyTypeId;
    private Supplier<BufferPool> supplyBufferPool;

    public HttpServerFactoryBuilder(
        HttpConfiguration config)
    {
        this.config = config;
    }

    @Override
    public HttpServerFactoryBuilder setRouteManager(
        RouteManager router)
    {
        this.router = router;
        return this;
    }

    @Override
    public HttpServerFactoryBuilder setWriteBuffer(
        MutableDirectBuffer writeBuffer)
    {
        this.writeBuffer = writeBuffer;
        return this;
    }

    @Override
    public HttpServerFactoryBuilder setInitialIdSupplier(
        LongUnaryOperator supplyStreamId)
    {
        this.supplyInitialId = supplyStreamId;
        return this;
    }

    @Override
    public HttpServerFactoryBuilder setReplyIdSupplier(
        LongUnaryOperator supplyReplyId)
    {
        this.supplyReplyId = supplyReplyId;
        return this;
    }

    @Override
    public StreamFactoryBuilder setTypeIdSupplier(
        ToIntFunction<String> supplyTypeId)
    {
        this.supplyTypeId = supplyTypeId;
        return this;
    }

    @Override
    public StreamFactoryBuilder setBufferPoolSupplier(
        Supplier<BufferPool> supplyBufferPool)
    {
        this.supplyBufferPool = supplyBufferPool;
        return this;
    }

    @Override
    public StreamFactory build()
    {
        final BufferPool bufferPool = supplyBufferPool.get();

        return new HttpServerFactory(
                config,
                router,
                writeBuffer,
                bufferPool,
                supplyInitialId,
                supplyReplyId,
                supplyTypeId);
    }
}
