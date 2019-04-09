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

import static java.lang.System.currentTimeMillis;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.http.internal.util.HttpUtil.appendHeader;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http.internal.types.OctetsFW;
import org.reaktivity.nukleus.http.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http.internal.types.stream.FrameFW;
import org.reaktivity.nukleus.http.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.http.internal.types.stream.AbortFW;

public final class ServerConnectReplyStream implements MessageConsumer
{
    private static final Map<String, String> EMPTY_HEADERS = Collections.emptyMap();

    public static final byte[] RESPONSE_HEADERS_TOO_LONG_RESPONSE =
            "HTTP/1.1 507 Insufficient Storage\r\n\r\n".getBytes(US_ASCII);

    private final ServerStreamFactory factory;
    private final MessageConsumer connectReplyThrottle;
    private final long connectRouteId;
    private final long connectReplyId;

    private MessageConsumer streamState;
    private MessageConsumer throttleState;

    private ServerAcceptState acceptState;

    private int slotIndex;
    private int slotPosition;
    private int slotOffset;
    private boolean endDeferred;

    private int connectReplyBudget;
    private long traceId;

    public ServerConnectReplyStream(
        ServerStreamFactory factory,
        MessageConsumer connectReplyThrottle,
        long connectRouteId,
        long connectReplyId,
        long traceId)
    {
        this.factory = factory;
        this.connectReplyThrottle = connectReplyThrottle;
        this.connectRouteId = connectRouteId;
        this.connectReplyId = connectReplyId;
        this.traceId = traceId;

        this.streamState = this::streamBeforeBegin;
        this.throttleState = this::throttleBeforeBegin;
    }

    @Override
    public void accept(int msgTypeId, DirectBuffer buffer, int index, int length)
    {
        streamState.accept(msgTypeId, buffer, index, length);
    }

    public void handleThrottle(int msgTypeId, DirectBuffer buffer, int index, int length)
    {
        throttleState.accept(msgTypeId, buffer, index, length);
    }

    @Override
    public String toString()
    {
        return String.format("%s[connectReplyId=%016x, budget=%d, acceptState=%s]",
                getClass().getSimpleName(), connectReplyId, connectReplyBudget, acceptState);
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

    private void streamBeforeHeadersWritten(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case EndFW.TYPE_ID:
            endDeferred = true;
            break;
        default:
            factory.bufferPool.release(slotIndex);
            processUnexpected(buffer, index, length);
            break;
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
        case AbortFW.TYPE_ID:
            processAbort(buffer, index, length);
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

    private void processAbort(
        DirectBuffer buffer,
        int index,
        int length)
    {
        doCleanup();
    }

    private void doCleanup()
    {
        factory.writer.doReset(connectReplyThrottle, connectRouteId, connectReplyId, factory.supplyTrace.getAsLong());
        acceptState.doAbort(factory.writer, 0);
        this.streamState = null;
        releaseSlotIfNecessary();
    }

    private void processBegin(
        DirectBuffer buffer,
        int index,
        int length)
    {
        BeginFW begin = factory.beginRO.wrap(buffer, index, index + length);

        final long replyId = begin.streamId();
        final OctetsFW extension = begin.extension();
        traceId = begin.trace();

        @SuppressWarnings("unchecked")
        final Correlation<ServerAcceptState> correlation =
                     (Correlation<ServerAcceptState>) factory.correlations.remove(replyId);
        if (correlation != null)
        {
            acceptState = correlation.state();
            acceptState.setCleanupConnectReply.accept(this::doCleanup);

            Map<String, String> headers = EMPTY_HEADERS;
            if (extension.sizeof() > 0)
            {
                final HttpBeginExFW beginEx = extension.get(factory.beginExRO::wrap);
                Map<String, String> headers0 = new LinkedHashMap<>();
                beginEx.headers().forEach(h -> headers0.put(h.name().asString(), h.value().asString()));
                headers = headers0;
            }

            acceptState.setThrottle.accept(this::handleThrottle);

            // default status (and reason)
            String[] status = new String[] { "200", "OK" };

            StringBuilder headersChars = new StringBuilder();
            headers.forEach((name, value) ->
            {
                if (":status".equals(name))
                {
                    status[0] = value;
                    if ("101".equals(status[0]))
                    {
                        status[1] = "Switching Protocols";
                    }
                }
                else
                {
                    if ("connection".equals(name) && "close".equals(value))
                    {
                        acceptState.endRequested = true;
                    }

                    appendHeader(headersChars, name, value);
                }
            });

            String payloadChars =
                    new StringBuilder().append("HTTP/1.1 ").append(status[0]).append(" ").append(status[1]).append("\r\n")
                                       .append(headersChars).append("\r\n").toString();

            slotIndex = factory.bufferPool.acquire(connectReplyId);
            if (slotIndex == NO_SLOT)
            {
                factory.writer.doReset(connectReplyThrottle, connectRouteId, connectReplyId, factory.supplyTrace.getAsLong());
                this.streamState = null;
            }
            else
            {
                slotPosition = 0;
                MutableDirectBuffer slot = factory.bufferPool.buffer(slotIndex);
                if (payloadChars.length() > slot.capacity())
                {
                    slot.putBytes(0,  RESPONSE_HEADERS_TOO_LONG_RESPONSE);
                    acceptState.acceptReplyBudget -=
                            RESPONSE_HEADERS_TOO_LONG_RESPONSE.length + acceptState.acceptReplyPadding;
                    assert acceptState.acceptReplyBudget >= 0;
                    factory.writer.doData(acceptState.acceptReply, acceptState.acceptRouteId, acceptState.replyStreamId, traceId,
                            acceptState.acceptReplyPadding, slot, 0, RESPONSE_HEADERS_TOO_LONG_RESPONSE.length);
                    factory.writer.doReset(connectReplyThrottle, connectRouteId, connectReplyId, factory.supplyTrace.getAsLong());
                }
                else
                {
                    byte[] bytes = payloadChars.getBytes(US_ASCII);
                    slot.putBytes(0, bytes);
                    slotPosition = bytes.length;
                    slotOffset = 0;
                    this.streamState = this::streamBeforeHeadersWritten;
                    this.throttleState = this::throttleBeforeHeadersWritten;
                    if (acceptState.acceptReplyBudget > 0)
                    {
                        useTargetWindowToWriteResponseHeaders();
                    }
                }
            }
        }
        else
        {
            processUnexpected(buffer, index, length);
        }
    }

    private void processData(
        DirectBuffer buffer,
        int index,
        int length)
    {
        DataFW data = factory.dataRO.wrap(buffer, index, index + length);
        connectReplyBudget -= data.length() + data.padding();
        long traceId = data.trace();

        if (connectReplyBudget < 0)
        {
            processUnexpected(buffer, index, length);
        }
        else
        {
            final OctetsFW payload = data.payload();
            acceptState.acceptReplyBudget -= payload.sizeof() + acceptState.acceptReplyPadding;

            if (acceptState.acceptReplyBudget < 0)
            {
                 String assertionErrorMessage = String.format("[%016x] %s, payload=%d",
                                                        currentTimeMillis(),
                                                        this,
                                                        payload.sizeof());
                throw new AssertionError(assertionErrorMessage);
            }
            factory.writer.doData(acceptState.acceptReply, acceptState.acceptRouteId, acceptState.replyStreamId, traceId,
                    acceptState.acceptReplyPadding, payload);
        }
    }

    private void processEnd(
        DirectBuffer buffer,
        int index,
        int length)
    {
        EndFW end = factory.endRO.wrap(buffer, index, index + length);
        doEnd(end.trace());
    }

    private void doEnd(long traceId)
    {
        if (acceptState != null && --acceptState.pendingRequests == 0 && acceptState.endRequested)
        {
            factory.writer.doEnd(acceptState.acceptReply, acceptState.acceptRouteId, acceptState.replyStreamId, traceId);
            acceptState.restoreInitialThrottle();
            this.streamState = this::streamAfterEnd;
            releaseSlotIfNecessary();
        }
        else
        {
            throttleState = this::throttleBetweenResponses;
            streamState = this::streamBeforeBegin;
        }
    }

    private void processUnexpected(
        DirectBuffer buffer,
        int index,
        int length)
    {
        FrameFW frame = factory.frameRO.wrap(buffer, index, index + length);

        final long routeId = frame.routeId();
        final long streamId = frame.streamId();

        factory.writer.doReset(connectReplyThrottle, routeId, streamId, factory.supplyTrace.getAsLong());

        this.streamState = null;
    }

    private void throttleBeforeBegin(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case ResetFW.TYPE_ID:
            ResetFW reset = factory.resetRO.wrap(buffer, index, index + length);
            processReset(reset);
            break;
        default:
            // ignore
            break;
        }
    }

    private void throttleBeforeHeadersWritten(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case WindowFW.TYPE_ID:
            WindowFW window = factory.windowRO.wrap(buffer, index, index + length);
            acceptState.acceptReplyBudget += window.credit();
            acceptState.acceptReplyPadding = window.padding();
            useTargetWindowToWriteResponseHeaders();
            break;
        case ResetFW.TYPE_ID:
            ResetFW reset = factory.resetRO.wrap(buffer, index, index + length);
            processReset(reset);
            break;
        default:
            // ignore
            break;
        }
    }

    private void throttleBetweenResponses(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case WindowFW.TYPE_ID:
            WindowFW window = factory.windowRO.wrap(buffer, index, index + length);
            acceptState.acceptReplyBudget += window.credit();
            acceptState.acceptReplyPadding = window.padding();
            break;
        case ResetFW.TYPE_ID:
            ResetFW reset = factory.resetRO.wrap(buffer, index, index + length);
            processReset(reset);
            break;
        default:
            // ignore
            break;
        }
    }

    private void throttleNextWindow(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case WindowFW.TYPE_ID:
            WindowFW window = factory.windowRO.wrap(buffer, index, index + length);
            processWindow(window);
            break;
        case ResetFW.TYPE_ID:
            ResetFW reset = factory.resetRO.wrap(buffer, index, index + length);
            processReset(reset);
            break;
        default:
            // ignore
            break;
        }
    }

    private void useTargetWindowToWriteResponseHeaders()
    {
        int bytesDeferred = slotPosition - slotOffset;
        int writableBytes = Math.min(bytesDeferred, acceptState.acceptReplyBudget - acceptState.acceptReplyPadding);

        if (writableBytes > 0)
        {
            MutableDirectBuffer slot = factory.bufferPool.buffer(slotIndex);
            factory.writer.doData(acceptState.acceptReply, acceptState.acceptRouteId, acceptState.replyStreamId, traceId,
                    acceptState.acceptReplyPadding, slot, slotOffset, writableBytes);
            acceptState.acceptReplyBudget -= writableBytes + acceptState.acceptReplyPadding;
            assert acceptState.acceptReplyBudget >= 0;
            slotOffset += writableBytes;
            bytesDeferred -= writableBytes;
            if (bytesDeferred == 0)
            {
                factory.bufferPool.release(slotIndex);
                slotIndex = NO_SLOT;
                if (endDeferred)
                {
                    doEnd(factory.supplyTrace.getAsLong());
                }
                else
                {
                    streamState = this::streamAfterBeginOrData;
                    throttleState = this::throttleNextWindow;
                }
            }
        }
    }

    private void processWindow(
        WindowFW window)
    {
        acceptState.acceptReplyBudget += window.credit();
        acceptState.acceptReplyPadding = window.padding();

        int connectReplyCredit = acceptState.acceptReplyBudget - connectReplyBudget;
        if (connectReplyCredit > 0)
        {
            connectReplyBudget += connectReplyCredit;
            int connectReplyPadding = acceptState.acceptReplyPadding;
            long traceId = window.trace();
            factory.writer.doWindow(connectReplyThrottle, connectRouteId, connectReplyId, traceId,
                    connectReplyCredit, connectReplyPadding);
        }
    }

    private void processReset(
        ResetFW reset)
    {
        releaseSlotIfNecessary();
        final long traceId = reset.trace();

        factory.writer.doReset(connectReplyThrottle, connectRouteId, connectReplyId, traceId);
    }

    private void releaseSlotIfNecessary()
    {
        if (slotIndex != NO_SLOT)
        {
            factory.bufferPool.release(slotIndex);
            slotIndex = NO_SLOT;
        }
    }
}

