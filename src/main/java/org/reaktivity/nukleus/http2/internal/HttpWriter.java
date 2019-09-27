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
package org.reaktivity.nukleus.http2.internal;

import java.util.function.Consumer;
import java.util.function.ToIntFunction;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http.internal.types.ArrayFW;
import org.reaktivity.nukleus.http.internal.types.Flyweight;
import org.reaktivity.nukleus.http.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http.internal.types.stream.HttpBeginExFW;

class HttpWriter
{
    private static final String HTTP_TYPE_NAME = "http";

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();
    private final HttpBeginExFW.Builder httpBeginExRW = new HttpBeginExFW.Builder();

    final int httpTypeId;
    private final MutableDirectBuffer writeBuffer;

    HttpWriter(
        ToIntFunction<String> supplyTypeId,
        MutableDirectBuffer writeBuffer)
    {
        this.httpTypeId = supplyTypeId.applyAsInt(HTTP_TYPE_NAME);
        this.writeBuffer = writeBuffer;
    }

    // HTTP begin frame's extension data is written using the given buffer
    void doHttpBegin(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        long affinity,
        DirectBuffer extBuffer,
        int extOffset,
        int extLength)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
                .affinity(affinity)
                .extension(extBuffer, extOffset, extLength)
                .build();

        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    void doHttpBegin(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        long authorization,
        Consumer<ArrayFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> mutator)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
                .authorization(authorization)
                .extension(e -> e.set(visitHttpBeginEx(mutator)))
                .build();

        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    void doHttpData(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        int padding,
        DirectBuffer payload,
        int offset,
        int length)
    {
        final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
                .groupId(0)
                .padding(padding)
                .payload(payload, offset, length)
                .build();

        receiver.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    void doHttpEnd(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId)
    {
        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
                .build();

        receiver.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    void doHttpAbort(
        final MessageConsumer receiver,
        final long routeId,
        final long streamId,
        final long traceId)
    {
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
                .build();

        receiver.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    private Flyweight.Builder.Visitor visitHttpBeginEx(
            Consumer<ArrayFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> headers)
    {
        return (buffer, offset, limit) ->
                httpBeginExRW.wrap(buffer, offset, limit)
                             .typeId(httpTypeId)
                             .headers(headers)
                             .build()
                             .sizeof();
    }
}
