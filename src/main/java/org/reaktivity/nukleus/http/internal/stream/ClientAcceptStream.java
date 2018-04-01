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

import static org.reaktivity.nukleus.buffer.BufferPool.NO_SLOT;
import static org.reaktivity.nukleus.http.internal.util.HttpUtil.appendHeader;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Consumer;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.buffer.BufferPool;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http.internal.stream.ConnectionPool.CloseAction;
import org.reaktivity.nukleus.http.internal.stream.ConnectionPool.Connection;
import org.reaktivity.nukleus.http.internal.stream.ConnectionPool.ConnectionRequest;
import org.reaktivity.nukleus.http.internal.types.OctetsFW;
import org.reaktivity.nukleus.http.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.http.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http.internal.types.stream.WindowFW;

final class ClientAcceptStream implements ConnectionRequest, Consumer<Connection>, MessageConsumer
{
    private final ClientStreamFactory factory;

    private MessageConsumer streamState;
    private MessageConsumer throttleState;

    private final long acceptId;
    private final String acceptName;
    private final long acceptCorrelationId;
    private final MessageConsumer acceptThrottle;
    private final String connectName;
    private final long connectRef;
    private Map<String, String> headers;
    private MessageConsumer target;
    private Connection connection;
    private ConnectionRequest nextConnectionRequest;
    private ConnectionPool connectionPool;
    private int sourceBudget;
    private int slotIndex;
    private int slotPosition;
    private int slotOffset;
    private boolean endDeferred;
    private boolean persistent = true;

    ClientAcceptStream(ClientStreamFactory factory, MessageConsumer acceptThrottle,
            long acceptId, long acceptRef, String acceptName, long acceptCorrelationId,
            String connectName, long connectRef, Map<String, String> headers)
    {
        this.factory = factory;
        this.acceptThrottle = acceptThrottle;
        this.acceptId = this.factory.beginRO.streamId();
        this.acceptName = acceptName;
        this.acceptCorrelationId = acceptCorrelationId;
        this.connectName = connectName;
        this.connectRef = connectRef;
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
            factory.writer.doWindow(acceptThrottle, streamId, data.length(), 0);
            break;
        case EndFW.TYPE_ID:
            factory.endRO.wrap(buffer, index, index + length);
            this.streamState = this::streamAfterEndOrAbort;
            releaseSlotIfNecessary();
            break;
        case AbortFW.TYPE_ID:
            factory.abortRO.wrap(buffer, index, index + length);
            this.streamState = this::streamAfterEndOrAbort;
            releaseSlotIfNecessary();
            break;
        }
    }

    private void processBegin(
        DirectBuffer buffer,
        int index,
        int length)
    {
        slotIndex = this.factory.bufferPool.acquire(acceptId);
        if (slotIndex == BufferPool.NO_SLOT)
        {
            factory.writer.doReset(acceptThrottle, acceptId);
            this.streamState = this::streamAfterReplyOrReset;
        }
        else
        {
            byte[] bytes = encodeHeaders(headers, buffer, index, length);
            headers = null; // allow gc
            slotPosition = 0;
            MutableDirectBuffer slot = factory.bufferPool.buffer(slotIndex);
            if (bytes.length > slot.capacity())
            {
                // TODO: diagnostics (reset reason?)
                factory.writer.doReset(acceptThrottle, acceptId);
                factory.bufferPool.release(slotIndex);
            }
            else
            {
                slot.putBytes(0, bytes);
                slotPosition = bytes.length;
                slotOffset = 0;
                this.streamState = this::streamBeforeHeadersWritten;
                this.throttleState = this::throttleBeforeHeadersWritten;
                target = factory.router.supplyTarget(connectName);
                connectionPool = getConnectionPool(connectName, connectRef);
                connectionPool.acquire(this);
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

    private ConnectionPool getConnectionPool(final String targetName, long targetRef)
    {
        Map<Long, ConnectionPool> connectionsByRef = this.factory.connectionPools.
                computeIfAbsent(targetName, (n) -> new Long2ObjectHashMap<ConnectionPool>());
        return connectionsByRef.computeIfAbsent(targetRef, (r) ->
            new ConnectionPool(factory, targetName, targetRef));
    }

    private void processData(
        DirectBuffer buffer,
        int index,
        int length)
    {
        DataFW data = factory.dataRO.wrap(buffer, index, index + length);

        sourceBudget -= data.length() + data.padding();
        if (sourceBudget < 0)
        {
            processUnexpected(buffer, index, length);
        }
        else
        {
            final OctetsFW payload = this.factory.dataRO.payload();
            factory.writer.doData(target, connection.connectStreamId, connection.padding, payload);
            connection.budget -= payload.sizeof();
        }
    }

    private void processEnd(
        DirectBuffer buffer,
        int index,
        int length)
    {
        this.factory.endRO.wrap(buffer, index, index + length);
        doEnd();
    }

    private void doEnd()
    {
        connectionPool.setDefaultThrottle(connection);
        this.streamState = this::streamAfterEndOrAbort;
        releaseSlotIfNecessary();
    }

    private void processUnexpected(
        DirectBuffer buffer,
        int index,
        int length)
    {
        this.factory.frameRO.wrap(buffer, index, index + length);

        final long streamId = this.factory.frameRO.streamId();

        factory.writer.doReset(acceptThrottle, streamId);

        this.streamState = this::streamAfterReplyOrReset;
        releaseSlotIfNecessary();
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
            System.out.println("Ignore");
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
            this.factory.windowRO.wrap(buffer, index, index + length);
            int credit = this.factory.windowRO.credit();
            int padding = this.factory.windowRO.padding();
            connection.budget += credit;
            connection.padding = padding;
            doSourceWindow(credit, padding);
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
        int writableBytes = Math.min(slotPosition - slotOffset, connection.budget);
        MutableDirectBuffer slot = this.factory.bufferPool.buffer(slotIndex);
        factory.writer.doData(target, connection.connectStreamId, connection.padding, slot, slotOffset, writableBytes);
        connection.budget -= writableBytes;
        slotOffset += writableBytes;
        int bytesDeferred = slotPosition - slotOffset;
        if (bytesDeferred == 0)
        {
            this.factory.bufferPool.release(slotIndex);
            slotIndex = BufferPool.NO_SLOT;
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
                    doSourceWindow(connection.budget, connection.padding);
                }
            }
        }
    }

    private void doSourceWindow(int credit, int padding)
    {
        sourceBudget += credit;
        factory.writer.doWindow(acceptThrottle, acceptId, credit, padding);
    }

    private void processAbort(
        DirectBuffer buffer,
        int index,
        int length)
    {
        factory.abortRO.wrap(buffer, index, index + length);
        releaseSlotIfNecessary();

        if (connection == null)
        {
            // request still enqueued, remove it from the queue
            connectionPool.cancel(this);
        }
        else
        {
            factory.correlations.remove(connection.correlationId);
            connection.persistent = false;
            connectionPool.release(connection, CloseAction.ABORT);
        }
    }

    private void processReset(
        DirectBuffer buffer,
        int index,
        int length)
    {
        factory.resetRO.wrap(buffer, index, index + length);
        releaseSlotIfNecessary();
        connection.persistent = false;
        connectionPool.release(connection);
        factory.writer.doReset(acceptThrottle, acceptId);
    }

    private void releaseSlotIfNecessary()
    {
        if (slotIndex != NO_SLOT)
        {
            factory.bufferPool.release(slotIndex);
            slotIndex = NO_SLOT;
        }
    }

    @Override
    public Consumer<Connection> getConsumer()
    {
        return this;
    }

    @Override
    public void next(ConnectionRequest request)
    {
        nextConnectionRequest = request;
    }

    @Override
    public ConnectionRequest next()
    {
        return nextConnectionRequest;
    }

    @Override
    public void accept(Connection connection)
    {
        this.connection = connection;
        connection.persistent = persistent;
        ClientConnectReplyState state = new ClientConnectReplyState(connectionPool, connection);
        final Correlation<ClientConnectReplyState> correlation =
                new Correlation<>(acceptCorrelationId, acceptName, state);
        factory.correlations.put(connection.correlationId, correlation);
        factory.router.setThrottle(connectName, connection.connectStreamId, this::handleThrottle);
        if (connection.budget > 0)
        {
            useWindowToWriteRequestHeaders();
        }
    }
}
