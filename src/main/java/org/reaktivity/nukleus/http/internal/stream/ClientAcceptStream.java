package org.reaktivity.nukleus.http.internal.stream;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.MessageHandler;
import org.reaktivity.nukleus.http.internal.stream.ConnectionPool.Connection;
import org.reaktivity.nukleus.http.internal.stream.ConnectionPool.ConnectionRequest;
import org.reaktivity.nukleus.http.internal.types.OctetsFW;
import org.reaktivity.nukleus.http.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http.internal.types.stream.WindowFW;
import org.reaktivity.reaktor.internal.acceptable.Target;

final class ClientAcceptStream implements ConnectionRequest, Consumer<Connection>
{
    private final ClientStreamFactory clientStreamFactory;

    private MessageHandler streamState;
    private MessageHandler throttleState;

    private long sourceId;
    private long sourceRef;
    private long correlationId;
    private Target target;
    private long targetRef;
    private Connection connection;
    private ConnectionRequest nextConnectionRequest;
    private ConnectionPool connectionPool;
    private int sourceWindow;
    private int slotIndex;
    private int slotPosition;
    private int slotOffset;
    private boolean endDeferred;
    private boolean persistent = true;

    ClientAcceptStream(ClientStreamFactory clientStreamFactory)
    {
        this.clientStreamFactory = clientStreamFactory;
        this.streamState = this::streamBeforeBegin;
        this.throttleState = this::throttleBeforeBegin;
    }

    void handleStream(
        int msgTypeId,
        MutableDirectBuffer buffer,
        int index,
        int length)
    {
        streamState.onMessage(msgTypeId, buffer, index, length);
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
            this.clientStreamFactory.slab.release(slotIndex);
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

    private void streamAfterReplyOrReset(
        int msgTypeId,
        MutableDirectBuffer buffer,
        int index,
        int length)
    {
        if (msgTypeId == DataFW.TYPE_ID)
        {
            this.clientStreamFactory.dataRO.wrap(buffer, index, index + length);
            final long streamId = this.clientStreamFactory.dataRO.streamId();

            source.doWindow(streamId, length);
        }
        else if (msgTypeId == EndFW.TYPE_ID)
        {
            this.clientStreamFactory.endRO.wrap(buffer, index, index + length);
            final long streamId = this.clientStreamFactory.endRO.streamId();

            source.removeStream(streamId);

            this.streamState = this::streamAfterEnd;
        }
    }

    private void processBegin(
        DirectBuffer buffer,
        int index,
        int length)
    {
        this.clientStreamFactory.beginRO.wrap(buffer, index, index + length);
        this.sourceId = this.clientStreamFactory.beginRO.streamId();
        this.sourceRef = this.clientStreamFactory.beginRO.sourceRef();
        this.correlationId = this.clientStreamFactory.beginRO.correlationId();
        final OctetsFW extension = this.clientStreamFactory.beginRO.extension();

        // TODO: avoid object creation
        Map<String, String> headers = ClientStreamFactory.EMPTY_HEADERS;
        if (extension.sizeof() > 0)
        {
            final HttpBeginExFW beginEx = extension.get(this.clientStreamFactory.beginExRO::wrap);
            Map<String, String> headers0 = new LinkedHashMap<>();
            beginEx.headers().forEach(h -> headers0.put(h.name().asString(), h.value().asString()));
            headers = headers0;
        }
        final Optional<Route> optional = resolveTarget(sourceRef, headers);

        if (optional.isPresent())
        {
            slotIndex = this.clientStreamFactory.slab.acquire(sourceId);
            if (slotIndex == NO_SLOT)
            {
                source.doReset(sourceId);
                this.streamState = this::streamAfterReplyOrReset;
            }
            else
            {
                byte[] bytes = encodeHeaders(headers, buffer, index, length);
                slotPosition = 0;
                MutableDirectBuffer slot = this.clientStreamFactory.slab.buffer(slotIndex);
                if (bytes.length > slot.capacity())
                {
                    // TODO: diagnostics (reset reason?)
                    source.doReset(sourceId);
                    source.removeStream(sourceId);
                }
                else
                {
                    slot.putBytes(0, bytes);
                    slotPosition = bytes.length;
                    slotOffset = 0;
                    this.streamState = this::streamBeforeHeadersWritten;
                    this.throttleState = this::throttleBeforeHeadersWritten;
                    final Route route = optional.get();
                    target = route.target();
                    this.targetRef = route.targetRef();
                    connectionPool = getConnectionPool(target, targetRef);
                    connectionPool.acquire(this);
                }
            }
        }
        else
        {
            processUnexpected(buffer, index, length);
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

        if (pseudoHeaders[ClientStreamFactory.METHOD] == null || pseudoHeaders[ClientStreamFactory.SCHEME] == null || pseudoHeaders[ClientStreamFactory.PATH] == null
                || pseudoHeaders[ClientStreamFactory.AUTHORITY] == null)
        {
            processUnexpected(buffer, index, length);
        }

        String payloadChars =
                new StringBuilder().append(pseudoHeaders[ClientStreamFactory.METHOD]).append(" ").append(pseudoHeaders[ClientStreamFactory.PATH])
                                   .append(" HTTP/1.1").append("\r\n")
                                   .append("Host").append(": ").append(pseudoHeaders[ClientStreamFactory.AUTHORITY]).append("\r\n")
                                   .append(headersChars).append("\r\n").toString();
        return payloadChars.getBytes(StandardCharsets.US_ASCII);
    }

    private ConnectionPool getConnectionPool(final Target target, long targetRef)
    {
        Map<Long, ConnectionPool> connectionsByRef = this.clientStreamFactory.connectionPools.
                computeIfAbsent(target.name(), (n) -> new Long2ObjectHashMap<ConnectionPool>());
        return connectionsByRef.computeIfAbsent(targetRef, (r) ->
            new ConnectionPool(this.clientStreamFactory.maximumConnectionsPerRoute, supplyTargetId, supplyTarget,
                    correlateEstablished, target, targetRef));
    }

    private void processData(
        DirectBuffer buffer,
        int index,
        int length)
    {
        this.clientStreamFactory.dataRO.wrap(buffer, index, index + length);

        sourceWindow -= this.clientStreamFactory.dataRO.length();
        if (sourceWindow < 0)
        {
            processUnexpected(buffer, index, length);
        }
        else
        {
            final OctetsFW payload = this.clientStreamFactory.dataRO.payload();
            target.doData(connection.outputStreamId, payload);
            connection.window -= payload.sizeof();
        }
    }

    private void processEnd(
        DirectBuffer buffer,
        int index,
        int length)
    {
        this.clientStreamFactory.endRO.wrap(buffer, index, index + length);
        doEnd();
    }

    private void doEnd()
    {
        target.removeThrottle(connection.outputStreamId);

        source.removeStream(sourceId);
        this.streamState = this::streamAfterEnd;
    }

    private void processUnexpected(
        DirectBuffer buffer,
        int index,
        int length)
    {
        this.clientStreamFactory.frameRO.wrap(buffer, index, index + length);

        final long streamId = this.clientStreamFactory.frameRO.streamId();

        source.doReset(streamId);

        this.streamState = this::streamAfterReplyOrReset;
    }

    private void handleThrottle(
        int msgTypeId,
        MutableDirectBuffer buffer,
        int index,
        int length)
    {
        throttleState.onMessage(msgTypeId, buffer, index, length);
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
            this.clientStreamFactory.windowRO.wrap(buffer, index, index + length);
            connection.window += this.clientStreamFactory.windowRO.update();
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
            this.clientStreamFactory.windowRO.wrap(buffer, index, index + length);
            int update = this.clientStreamFactory.windowRO.update();
            connection.window += update;
            doSourceWindow(update);
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
        int writableBytes = Math.min(slotPosition - slotOffset, connection.window);
        MutableDirectBuffer slot = this.clientStreamFactory.slab.buffer(slotIndex);
        target.doData(connection.outputStreamId, slot, slotOffset, writableBytes);
        connection.window -= writableBytes;
        slotOffset += writableBytes;
        int bytesDeferred = slotPosition - slotOffset;
        if (bytesDeferred == 0)
        {
            this.clientStreamFactory.slab.release(slotIndex);
            slotIndex = NO_SLOT;
            if (endDeferred)
            {
                doEnd();
            }
            else
            {
                streamState = this::streamAfterBeginOrData;
                throttleState = this::throttleNextWindow;
                if (connection.window > 0)
                {
                    doSourceWindow(connection.window);
                }
            }
        }
    }

    private void doSourceWindow(int update)
    {
        sourceWindow += update;
        source.doWindow(sourceId, update);
    }

    private void processReset(
        DirectBuffer buffer,
        int index,
        int length)
    {
        this.clientStreamFactory.resetRO.wrap(buffer, index, index + length);
        this.clientStreamFactory.slab.release(slotIndex);
        connection.persistent = false;
        connectionPool.release(connection, false);
        source.doReset(sourceId);
    }

    private Optional<Route> resolveTarget(
        long sourceRef,
        Map<String, String> headers)
    {
        final List<Route> routes = supplyRoutes.apply(sourceRef);
        final Predicate<Route> predicate = headersMatch(headers);

        return routes.stream().filter(predicate).findFirst();
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
        final long targetCorrelationId = connection.outputStreamId;
        ClientConnectReplyState state = new ClientConnectReplyState(connectionPool, connection);
        final Correlation<ClientConnectReplyState> correlation =
                new Correlation<>(correlationId, source.routableName(), INPUT_ESTABLISHED, state);
        correlateNew.accept(targetCorrelationId, correlation);
        target.setThrottle(connection.outputStreamId, this::handleThrottle);
        if (connection.window > 0)
        {
            useWindowToWriteRequestHeaders();
        }
    }
}