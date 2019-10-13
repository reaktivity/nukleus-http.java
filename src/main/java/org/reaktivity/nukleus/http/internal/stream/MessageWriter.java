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

import java.util.function.Consumer;
import java.util.function.ToIntFunction;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http.internal.HttpNukleus;
import org.reaktivity.nukleus.http.internal.types.ArrayFW;
import org.reaktivity.nukleus.http.internal.types.Flyweight;
import org.reaktivity.nukleus.http.internal.types.HttpHeaderFW;
import org.reaktivity.nukleus.http.internal.types.OctetsFW;
import org.reaktivity.nukleus.http.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http.internal.types.stream.WindowFW;

final class MessageWriter
{
    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();

    private AbortFW.Builder abortRW = new AbortFW.Builder();

    private final HttpBeginExFW.Builder httpBeginExRW = new HttpBeginExFW.Builder();

    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();

    private final int httpTypeId;
    private MutableDirectBuffer writeBuffer;

    MessageWriter(
        ToIntFunction<String> supplyTypeId,
        MutableDirectBuffer writeBuffer)
    {
        this.httpTypeId = supplyTypeId.applyAsInt(HttpNukleus.NAME);
        this.writeBuffer = writeBuffer;
    }

    void doBegin(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        long affinity)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .affinity(affinity)
                .build();

        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    void doData(
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
                .traceId(traceId)
                .budgetId(0)
                .reserved(length + padding)
                .payload(p -> p.set(payload, offset, length))
                .build();

        receiver.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    public void doData(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        int padding,
        OctetsFW payload)
    {
        final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .budgetId(0)
                .reserved(payload.sizeof() + padding)
                .payload(p -> p.set(payload))
                .build();

        receiver.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    void doEnd(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId)
    {
        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .build();

        receiver.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    void doAbort(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId)
    {
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .build();

        receiver.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    void doHttpBegin(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        long affinity,
        Consumer<ArrayFW.Builder<HttpHeaderFW.Builder, HttpHeaderFW>> mutator)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .affinity(affinity)
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
                .traceId(traceId)
                .budgetId(0)
                .reserved(length + padding)
                .payload(p -> p.set(payload, offset, length))
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
                .traceId(traceId)
                .build();

        receiver.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    void doWindow(
        final MessageConsumer sender,
        final long routeId,
        final long streamId,
        final long traceId,
        final int credit,
        final int padding)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .budgetId(0L)
                .credit(credit)
                .padding(padding)
                .build();

        sender.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    void doReset(
        final MessageConsumer sender,
        final long routeId,
        final long streamId,
        final long traceId)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .build();

        sender.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
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
