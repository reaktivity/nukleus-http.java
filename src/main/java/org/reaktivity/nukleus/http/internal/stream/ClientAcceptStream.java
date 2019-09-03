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

import static org.reaktivity.nukleus.http.internal.util.HttpUtil.appendHeader;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Consumer;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http.internal.stream.ConnectionPool.CloseAction;
import org.reaktivity.nukleus.http.internal.stream.ConnectionPool.Connection;
import org.reaktivity.nukleus.http.internal.stream.ConnectionPool.ConnectionRequest;
import org.reaktivity.nukleus.http.internal.types.OctetsFW;
import org.reaktivity.nukleus.http.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http.internal.types.stream.FrameFW;
import org.reaktivity.nukleus.http.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http.internal.types.stream.WindowFW;

final class ClientAcceptStream implements ConnectionRequest, Consumer<Connection>, MessageConsumer
{
    private final ClientStreamFactory factory;

    private MessageConsumer streamState;
    private MessageConsumer throttleState;

    private final MessageConsumer acceptReply;
    private final long acceptRouteId;
    private final long acceptInitialId;
    private final long acceptReplyId;
    private final long connectRouteId;

    private Map<String, String> headers;
    private Connection connection;
    private ConnectionPool connectionPool;
    private int sourceBudget;
    private DirectBuffer headersBuffer;
    private int headersPosition;
    private int headersOffset;
    private boolean endDeferred;
    private boolean persistent = true;
    private long traceId;

    ClientAcceptStream(
        ClientStreamFactory factory,
        MessageConsumer acceptReply,
        long acceptRouteId,
        long acceptId,
        long acceptReplyId,
        long connectRouteId,
        Map<String, String> headers)
    {
        this.factory = factory;
        this.acceptReply = acceptReply;
        this.acceptRouteId = acceptRouteId;
        this.acceptInitialId = acceptId;
        this.acceptReplyId = acceptReplyId;
        this.connectRouteId = connectRouteId;
        this.headers = headers;
        this.streamState = this::streamBeforeBegin;
        this.throttleState = this::throttleBeforeBegin;
    }

    @Override
    public void accept(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        streamState.accept(msgTypeId, buffer, index, length);
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
        case AbortFW.TYPE_ID:
            processAbort(buffer, index, length);
            break;
        default:
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

    private void streamAfterEndOrAbort(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        processUnexpected(buffer, index, length);
    }

    private void streamAfterReplyOrReset(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case DataFW.TYPE_ID:
            DataFW data = this.factory.dataRO.wrap(buffer, index, index + length);
            final long streamId = data.streamId();
            factory.writer.doWindow(acceptReply, acceptRouteId, streamId, factory.supplyTrace.getAsLong(), data.length(), 0);
            break;
        case EndFW.TYPE_ID:
            factory.endRO.wrap(buffer, index, index + length);
            this.streamState = this::streamAfterEndOrAbort;
            break;
        case AbortFW.TYPE_ID:
            factory.abortRO.wrap(buffer, index, index + length);
            this.streamState = this::streamAfterEndOrAbort;
            break;
        }
    }

    private void processBegin(
        DirectBuffer buffer,
        int index,
        int length)
    {
        // count all requests
        factory.countRequests.getAsLong();
        byte[] bytes = encodeHeaders(headers, buffer, index, length);
        headers = null; // allow gc
        headersPosition = 0;
        if (bytes.length > factory.bufferPool.slotCapacity())
        {
            // TODO: diagnostics (reset reason?)
            factory.writer.doReset(acceptReply, acceptRouteId, acceptInitialId, factory.supplyTrace.getAsLong());
        }
        else
        {
            traceId = factory.frameRO.wrap(buffer, index, index + length).trace();
            headersBuffer = new UnsafeBuffer(bytes);
            headersPosition = bytes.length;
            headersOffset = 0;
            this.streamState = this::streamBeforeHeadersWritten;
            this.throttleState = this::throttleBeforeHeadersWritten;
            connectionPool = getConnectionPool(connectRouteId);
            boolean acquired = connectionPool.acquire(this);
            // No backend connection or cannot store in queue, send 503 with Retry-After
            if (!acquired)
            {
                // count all responses
                factory.countResponses.getAsLong();

                factory.writer.doWindow(acceptReply, acceptRouteId, acceptInitialId, traceId, 0, 0);

                factory.writer.doHttpBegin(acceptReply, acceptRouteId, acceptReplyId, factory.supplyTrace.getAsLong(),
                        hs -> hs.item(h -> h.name(":status").value("503"))
                                .item(h -> h.name("retry-after").value("0")));
                factory.writer.doHttpEnd(acceptReply, acceptRouteId, acceptReplyId, factory.supplyTrace.getAsLong());

                // count rejected requests (no connection or no space in the queue)
                factory.countRequestsRejected.getAsLong();
            }
        }

    }

    private byte[] encodeHeaders(Map<String, String> headers,
                                 DirectBuffer buffer,
                                 int index,
                                 int length)
    {
        String[] pseudoHeaders = new String[4];

        StringBuilder headersChars = new StringBuilder();
        headers.forEach((name, value) ->
        {
            switch(name.toLowerCase())
            {
            case ":method":
                pseudoHeaders[ClientStreamFactory.METHOD] = value;
                switch(value.toLowerCase())
                {
                case "post":
                case "insert":
                    this.persistent = false;
                }
                break;
            case ":scheme":
                pseudoHeaders[ClientStreamFactory.SCHEME] = value;
                break;
            case ":authority":
                pseudoHeaders[ClientStreamFactory.AUTHORITY] = value;
                break;
            case ":path":
                pseudoHeaders[ClientStreamFactory.PATH] = value;
                break;
            case "host":
                if (pseudoHeaders[ClientStreamFactory.AUTHORITY] == null)
                {
                    pseudoHeaders[ClientStreamFactory.AUTHORITY] = value;
                }
                else if (!pseudoHeaders[ClientStreamFactory.AUTHORITY].equals(value))
                {
                    processUnexpected(buffer, index, length);
                }
                break;
            case "connection":
                Arrays.asList(value.toLowerCase().split(",")).stream().forEach((element) ->
                {
                    switch(element)
                    {
                    case "close":
                        this.persistent = false;
                        break;
                    }
                });
                appendHeader(headersChars, name, value);
                break;
            default:
                appendHeader(headersChars, name, value);
            }
        });

        if (pseudoHeaders[ClientStreamFactory.METHOD] == null ||
            pseudoHeaders[ClientStreamFactory.SCHEME] == null ||
            pseudoHeaders[ClientStreamFactory.PATH] == null ||
            pseudoHeaders[ClientStreamFactory.AUTHORITY] == null)
        {
            processUnexpected(buffer, index, length);
        }

        String payloadChars = new StringBuilder()
                   .append(pseudoHeaders[ClientStreamFactory.METHOD]).append(" ").append(pseudoHeaders[ClientStreamFactory.PATH])
                   .append(" HTTP/1.1").append("\r\n")
                   .append("Host").append(": ").append(pseudoHeaders[ClientStreamFactory.AUTHORITY]).append("\r\n")
                   .append(headersChars).append("\r\n").toString();
        return payloadChars.getBytes(StandardCharsets.US_ASCII);
    }

    private ConnectionPool getConnectionPool(
        long targetRouteId)
    {
        return factory.connectionPools.computeIfAbsent(targetRouteId, r -> new ConnectionPool(factory, r));
    }

    private void processData(
        DirectBuffer buffer,
        int index,
        int length)
    {
        DataFW data = factory.dataRO.wrap(buffer, index, index + length);
        final long traceId = data.trace();

        sourceBudget -= data.length() + data.padding();
        if (sourceBudget < 0)
        {
            processUnexpected(buffer, index, length);
        }
        else
        {
            final OctetsFW payload = this.factory.dataRO.payload();
            factory.writer.doData(connection.connectInitial, connectRouteId, connection.connectInitialId,
                    traceId, connection.padding, payload);
            connection.budget -= payload.sizeof() + connection.padding;
            assert connection.budget >= 0;
        }
    }

    private void processEnd(
        DirectBuffer buffer,
        int index,
        int length)
    {
        final EndFW end = factory.endRO.wrap(buffer, index, index + length);
        traceId = end.trace();
        doEnd();
    }

    private void doEnd()
    {
        if (connection.upgraded)
        {
            connectionPool.release(connection, CloseAction.END);
        }
        else
        {
            connectionPool.setDefaultThrottle(connection);
        }
        this.streamState = this::streamAfterEndOrAbort;
    }

    private void processUnexpected(
        DirectBuffer buffer,
        int index,
        int length)
    {
        FrameFW frame = this.factory.frameRO.wrap(buffer, index, index + length);
        final long streamId = frame.streamId();

        factory.writer.doReset(acceptReply, acceptRouteId, streamId, factory.supplyTrace.getAsLong());

        this.streamState = this::streamAfterReplyOrReset;
    }

    private void handleThrottle(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        throttleState.accept(msgTypeId, buffer, index, length);
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
            this.factory.windowRO.wrap(buffer, index, index + length);
            connection.budget += this.factory.windowRO.credit();
            connection.padding = this.factory.windowRO.padding();
            useWindowToWriteRequestHeaders();
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
            WindowFW windowFW = this.factory.windowRO.wrap(buffer, index, index + length);
            int credit = this.factory.windowRO.credit();
            int padding = this.factory.windowRO.padding();
            connection.budget += credit;
            connection.padding = padding;
            doSourceWindow(padding, windowFW.trace());
            break;
        case ResetFW.TYPE_ID:
            processReset(buffer, index, length);
            break;
        default:
            // ignore
            break;
        }
    }

    private void useWindowToWriteRequestHeaders()
    {
        int writableBytes = Math.min(headersPosition - headersOffset, connection.budget - connection.padding);
        if (writableBytes > 0)
        {
            factory.writer.doData(connection.connectInitial, connectRouteId, connection.connectInitialId, traceId,
                    connection.padding, headersBuffer, headersOffset, writableBytes);
            connection.budget -= writableBytes + connection.padding;
            assert connection.budget >= 0;
            headersOffset += writableBytes;
            int bytesDeferred = headersPosition - headersOffset;
            if (bytesDeferred == 0)
            {
                if (endDeferred)
                {
                    doEnd();
                }
                else
                {
                    streamState = this::streamAfterBeginOrData;
                    throttleState = this::throttleNextWindow;
                    if (connection.budget > 0)
                    {
                        doSourceWindow(connection.padding, factory.supplyTrace.getAsLong());
                    }
                }
            }
        }
    }

    private void doSourceWindow(int padding, long traceId)
    {
        int credit = connection.budget - sourceBudget;
        if (credit > 0)
        {
            sourceBudget += credit;
            factory.writer.doWindow(acceptReply, acceptRouteId, acceptInitialId, traceId, credit, padding);
        }
    }

    private void processAbort(
        DirectBuffer buffer,
        int index,
        int length)
    {
        factory.abortRO.wrap(buffer, index, index + length);

        if (connection == null)
        {
            // request still enqueued, remove it from the queue
            connectionPool.cancel(this);
        }
        else
        {
            factory.correlations.remove(connection.connectReplyId);
            connection.persistent = false;
            connectionPool.release(connection, CloseAction.ABORT);
        }
    }

    private void processReset(
        DirectBuffer buffer,
        int index,
        int length)
    {
        ResetFW resetFW = factory.resetRO.wrap(buffer, index, index + length);
        connection.persistent = false;
        connectionPool.release(connection);
        factory.writer.doReset(acceptReply, acceptRouteId, acceptInitialId, resetFW.trace());
    }

    @Override
    public Consumer<Connection> getConsumer()
    {
        return this;
    }

    @Override
    public void accept(Connection connection)
    {
        this.connection = connection;
        connection.persistent = persistent;
        ClientConnectReplyState state = new ClientConnectReplyState(connectionPool, connection);
        final Correlation<ClientConnectReplyState> correlation =
                new Correlation<>(acceptReply, acceptRouteId, acceptReplyId, state);
        factory.correlations.put(connection.connectReplyId, correlation);
        factory.router.setThrottle(connection.connectInitialId, this::handleThrottle);
        if (connection.budget > 0)
        {
            useWindowToWriteRequestHeaders();
        }
    }
}
