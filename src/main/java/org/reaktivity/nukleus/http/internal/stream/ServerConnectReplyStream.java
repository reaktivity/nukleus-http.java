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
import static org.reaktivity.nukleus.http.internal.routable.stream.Slab.NO_SLOT;
import static org.reaktivity.nukleus.http.internal.util.HttpUtil.appendHeader;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http.internal.routable.Correlation;
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

    private final FrameFW frameRO = new FrameFW();

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();

    private final HttpBeginExFW beginExRO = new HttpBeginExFW();

    private final MessageWriter writer;
    private final BufferPool slab;
    private final Long2ObjectHashMap<Correlation<?>> correlations;
    private final MessageConsumer connectReplyThrottle;
    private final long connectReplyId;
    private final String connectReplyName;

    private MessageConsumer streamState;
    private MessageConsumer throttleState;

    private ServerAcceptState targetStream;

    private int slotIndex;
    private int slotPosition;
    private int slotOffset;
    private boolean endDeferred;


    public ServerConnectReplyStream(
        MessageWriter writer,
        BufferPool slab,
        Long2ObjectHashMap<Correlation<?>> correlations,
        MessageConsumer connectReplyThrottle,
        long connectReplyId,
        String connectReplyName)
    {
        this.writer = writer;
        this.slab = slab;
        this.correlations = correlations;
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

    @Override
    public String toString()
    {
        return String.format("%s[source=%s, connectReplyId=%016x, window=%d, targetStream=%s]",
                getClass().getSimpleName(), connectReplyName, connectReplyId, targetStream);
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
            slab.release(slotIndex);
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
            dataRO.wrap(buffer, index, index + length);
            final long streamId = dataRO.streamId();
            writer.doWindow(connectReplyThrottle, streamId, length, length);
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
        beginRO.wrap(buffer, index, index + length);

        final long sourceRef = beginRO.sourceRef();
        final long targetCorrelationId = beginRO.correlationId();
        final OctetsFW extension = beginRO.extension();

        @SuppressWarnings("unchecked")
        final Correlation<ServerAcceptState> correlation =
                     (Correlation<ServerAcceptState>) correlations.remove(targetCorrelationId);

        if (sourceRef == 0L && correlation != null)
        {
            targetStream = correlation.state();

            Map<String, String> headers = EMPTY_HEADERS;
            if (extension.sizeof() > 0)
            {
                final HttpBeginExFW beginEx = extension.get(beginExRO::wrap);
                Map<String, String> headers0 = new LinkedHashMap<>();
                beginEx.headers().forEach(h -> headers0.put(h.name().asString(), h.value().asString()));
                headers = headers0;
            }

            targetStream.setThrottle.accept(throttleState);

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

            slotIndex = slab.acquire(connectReplyId);
            if (slotIndex == NO_SLOT)
            {
                writer.doReset(connectReplyThrottle, connectReplyId);
                this.streamState = this::streamAfterRejectOrReset;
            }
            else
            {
                slotPosition = 0;
                MutableDirectBuffer slot = slab.buffer(slotIndex);
                if (payloadChars.length() > slot.capacity())
                {
                    slot.putBytes(0,  RESPONSE_HEADERS_TOO_LONG_RESPONSE);
                    writer.doData(targetStream.acceptReply, targetStream.replyStreamId,
                                  slot, 0, RESPONSE_HEADERS_TOO_LONG_RESPONSE.length);
                    writer.doReset(connectReplyThrottle, connectReplyId);
                }
                else
                {
                    byte[] bytes = payloadChars.getBytes(US_ASCII);
                    slot.putBytes(0, bytes);
                    slotPosition = bytes.length;
                    slotOffset = 0;
                    this.streamState = this::streamBeforeHeadersWritten;
                    this.throttleState = this::throttleBeforeHeadersWritten;
                    targetStream.setThrottle.accept(throttleState);
                    if (targetStream.window > 0)
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

        dataRO.wrap(buffer, index, index + length);

        if (targetStream.window < dataRO.length())
        {
            processUnexpected(buffer, index, length);
        }
        else
        {
            final OctetsFW payload = dataRO.payload();
            writer.doData(targetStream.acceptReply, targetStream.replyStreamId, payload);
            targetStream.window -= dataRO.length();
        }
    }

    private void processEnd(
        DirectBuffer buffer,
        int index,
        int length)
    {
        endRO.wrap(buffer, index, index + length);
        doEnd();
    }

    private void doEnd()
    {
        if (targetStream != null && targetStream.endRequested && --targetStream.pendingRequests == 0)
        {
            writer.doEnd(targetStream.acceptReply, targetStream.replyStreamId);
            targetStream.restoreInitialThrottle();
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
        frameRO.wrap(buffer, index, index + length);

        final long streamId = frameRO.streamId();

        writer.doReset(connectReplyThrottle, streamId);

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
            processReset(buffer, index, length);
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
            windowRO.wrap(buffer, index, index + length);
            int update = windowRO.update();
            targetStream.window += update;
            useTargetWindowToWriteResponseHeaders();
            break;
        case ResetFW.TYPE_ID:
            processReset(buffer, index, length);
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
            windowRO.wrap(buffer, index, index + length);
            int update = windowRO.update();
            targetStream.window += update;
            break;
        case ResetFW.TYPE_ID:
            processReset(buffer, index, length);
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
            processWindow(buffer, index, length);
            break;
        case ResetFW.TYPE_ID:
            processReset(buffer, index, length);
            break;
        default:
            // ignore
            break;
        }
    }

    private void useTargetWindowToWriteResponseHeaders()
    {
        int bytesDeferred = slotPosition - slotOffset;
        int writableBytes = Math.min(bytesDeferred, targetStream.window);
        MutableDirectBuffer slot = slab.buffer(slotIndex);
        writer.doData(targetStream.acceptReply, targetStream.replyStreamId, slot, slotOffset, writableBytes);
        targetStream.window -= writableBytes;
        slotOffset += writableBytes;
        bytesDeferred -= writableBytes;
        if (bytesDeferred == 0)
        {
            slab.release(slotIndex);
            slotIndex = NO_SLOT;
            if (endDeferred)
            {
                doEnd();
            }
            else
            {
                streamState = this::streamAfterBeginOrData;
                throttleState = this::throttleNextWindow;
                if (targetStream.window > 0)
                {
                    doSourceWindow(targetStream.window);
                }
            }
        }
    }

    private void processWindow(
        DirectBuffer buffer,
        int index,
        int length)
    {
        windowRO.wrap(buffer, index, index + length);
        final int update = windowRO.update();
        targetStream.window += update;
        doSourceWindow(update);
    }

    private void doSourceWindow(int update)
    {
        writer.doWindow(connectReplyThrottle, connectReplyId, update, update);
    }

    private void processReset(
        DirectBuffer buffer,
        int index,
        int length)
    {
        resetRO.wrap(buffer, index, index + length);
        slab.release(slotIndex);

        writer.doReset(connectReplyThrottle, connectReplyId);
    }
}

