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

public final class ServerConnectReplyStream implements MessageConsumer
{
    private static final Map<String, String> EMPTY_HEADERS = Collections.emptyMap();

    public static final byte[] RESPONSE_HEADERS_TOO_LONG_RESPONSE =
            "HTTP/1.1 507 Insufficient Storage\r\n\r\n".getBytes(US_ASCII);

    private final ServerStreamFactory factory;
    private final MessageConsumer connectReplyThrottle;
    private final long connectReplyId;
    private final String connectReplyName;

    private MessageConsumer streamState;
    private MessageConsumer throttleState;

    private ServerAcceptState acceptState;

    private int slotIndex;
    private int slotPosition;
    private int slotOffset;
    private boolean endDeferred;

    private int connectReplyBudget;

    public ServerConnectReplyStream(
        ServerStreamFactory factory,
        MessageConsumer connectReplyThrottle,
        long connectReplyId,
        String connectReplyName)
    {
        this.factory = factory;
        this.connectReplyThrottle = connectReplyThrottle;
        this.connectReplyId = connectReplyId;
        this.connectReplyName = connectReplyName;

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
        return String.format("%s[source=%s, connectReplyId=%016x, window=%d, targetStream=%s]",
                getClass().getSimpleName(), connectReplyName, connectReplyId, acceptState);
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

    private void streamAfterRejectOrReset(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        if (msgTypeId == DataFW.TYPE_ID)
        {
            DataFW data = factory.dataRO.wrap(buffer, index, index + length);
            final long streamId = data.streamId();
            connectReplyBudget += data.length();
            factory.writer.doWindow(connectReplyThrottle, streamId, data.length() + data.padding(), 0);
        }
        else if (msgTypeId == EndFW.TYPE_ID)
        {
            this.streamState = this::streamAfterEnd;
        }
    }

    private void processBegin(
        DirectBuffer buffer,
        int index,
        int length)
    {
        BeginFW begin =factory.beginRO.wrap(buffer, index, index + length);

        final long sourceRef = begin.sourceRef();
        final long targetCorrelationId = begin.correlationId();
        final OctetsFW extension = begin.extension();

        @SuppressWarnings("unchecked")
        final Correlation<ServerAcceptState> correlation =
                     (Correlation<ServerAcceptState>) factory.correlations.remove(targetCorrelationId);

        if (sourceRef == 0L && correlation != null)
        {
            acceptState = correlation.state();

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
                    appendHeader(headersChars, name, value);
                }
            });

            String payloadChars =
                    new StringBuilder().append("HTTP/1.1 ").append(status[0]).append(" ").append(status[1]).append("\r\n")
                                       .append(headersChars).append("\r\n").toString();

            slotIndex = factory.bufferPool.acquire(connectReplyId);
            if (slotIndex == NO_SLOT)
            {
                factory.writer.doReset(connectReplyThrottle, connectReplyId);
                this.streamState = this::streamAfterRejectOrReset;
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
                    factory.writer.doData(acceptState.acceptReply, acceptState.replyStreamId,
                            acceptState.acceptReplyPadding, slot, 0, RESPONSE_HEADERS_TOO_LONG_RESPONSE.length);
                    factory.writer.doReset(connectReplyThrottle, connectReplyId);
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

        if (connectReplyBudget < 0)
        {
            processUnexpected(buffer, index, length);
        }
        else
        {
            final OctetsFW payload = data.payload();
            acceptState.acceptReplyBudget -= payload.sizeof() + acceptState.acceptReplyPadding;
            assert acceptState.acceptReplyBudget >= 0;
            factory.writer.doData(acceptState.acceptReply, acceptState.replyStreamId,
                    acceptState.acceptReplyPadding, payload);
        }
    }

    private void processEnd(
        DirectBuffer buffer,
        int index,
        int length)
    {
        factory.endRO.wrap(buffer, index, index + length);
        doEnd();
    }

    private void doEnd()
    {
        if (acceptState != null && acceptState.endRequested && --acceptState.pendingRequests == 0)
        {
            factory.writer.doEnd(acceptState.acceptReply, acceptState.replyStreamId);
            acceptState.restoreInitialThrottle();
            this.streamState = this::streamAfterEnd;
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

        final long streamId = frame.streamId();

        factory.writer.doReset(connectReplyThrottle, streamId);

        this.streamState = this::streamAfterRejectOrReset;
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
            factory.writer.doData(acceptState.acceptReply, acceptState.replyStreamId, acceptState.acceptReplyPadding,
                    slot, slotOffset, writableBytes);
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
                    doEnd();
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

        int connectReplyWindowCredit = acceptState.acceptReplyBudget - connectReplyBudget;
        if (connectReplyWindowCredit > 0)
        {
            connectReplyBudget += connectReplyWindowCredit;
            int connectReplyPadding = acceptState.acceptReplyPadding;
            factory.writer.doWindow(connectReplyThrottle, connectReplyId, connectReplyWindowCredit, connectReplyPadding);
        }
    }

    private void processReset(
        ResetFW reset)
    {
        releaseSlotIfNecessary();

        factory.writer.doReset(connectReplyThrottle, connectReplyId);
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

