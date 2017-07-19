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
package org.reaktivity.nukleus.http.internal.routable.stream;

import java.util.Collections;
import java.util.Map;

import org.reaktivity.nukleus.http.internal.types.stream.FrameFW;

public final class SourceOutputStreamFactory
{
    private static final Map<String, String> EMPTY_HEADERS = Collections.emptyMap();

    // Pseudo-headers
    private static final int METHOD = 0;
    private static final int SCHEME = 1;
    private static final int AUTHORITY = 2;
    private static final int PATH = 3;

    private final FrameFW frameRO = new FrameFW();

    private final BeginFW beginRO = new BeginFW();
    private final HttpBeginExFW beginExRO = new HttpBeginExFW();

    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();

    private final Source source;
    private final LongFunction<List<Route>> supplyRoutes;
    private final LongSupplier supplyTargetId;
    private final Function<String, Target> supplyTarget;
    private final LongFunction<Correlation<?>> correlateEstablished;

    private final LongObjectBiConsumer<Correlation<?>> correlateNew;

    private final Map<String, Map<Long, ConnectionPool>> connectionPools;
    private final int maximumConnectionsPerRoute;

    private final Slab slab;

    public SourceOutputStreamFactory(
        Source source,
        LongFunction<List<Route>> supplyRoutes,
        LongSupplier supplyTargetId,
        Function<String, Target> supplyTarget,
        LongFunction<Correlation<?>> correlateEstablished,
        LongObjectBiConsumer<Correlation<?>> correlateNew,
        Slab slab,
        int maximumConnectionsPerRoute)
    {
        this.source = source;
        this.supplyRoutes = supplyRoutes;
        this.supplyTargetId = supplyTargetId;
        this.supplyTarget = supplyTarget;
        this.correlateEstablished = correlateEstablished;
        this.correlateNew = correlateNew;
        this.slab = slab;
        this.connectionPools = new HashMap<>();
        this.maximumConnectionsPerRoute = maximumConnectionsPerRoute;
    }

    public MessageHandler newStream()
    {
        return new SourceOutputStream()::handleStream;
    }

    private final class SourceOutputStream implements ConnectionRequest, Consumer<Connection>
    {
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

        private SourceOutputStream()
        {
            this.streamState = this::streamBeforeBegin;
            this.throttleState = this::throttleBeforeBegin;
        }

        private void handleStream(
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

        private void streamAfterReplyOrReset(
            int msgTypeId,
            MutableDirectBuffer buffer,
            int index,
            int length)
        {
            if (msgTypeId == DataFW.TYPE_ID)
            {
                dataRO.wrap(buffer, index, index + length);
                final long streamId = dataRO.streamId();

                source.doWindow(streamId, length);
            }
            else if (msgTypeId == EndFW.TYPE_ID)
            {
                endRO.wrap(buffer, index, index + length);
                final long streamId = endRO.streamId();

                source.removeStream(streamId);

                this.streamState = this::streamAfterEnd;
            }
        }

        private void processBegin(
            DirectBuffer buffer,
            int index,
            int length)
        {
            beginRO.wrap(buffer, index, index + length);
            this.sourceId = beginRO.streamId();
            this.sourceRef = beginRO.sourceRef();
            this.correlationId = beginRO.correlationId();
            final OctetsFW extension = beginRO.extension();

            // TODO: avoid object creation
            Map<String, String> headers = EMPTY_HEADERS;
            if (extension.sizeof() > 0)
            {
                final HttpBeginExFW beginEx = extension.get(beginExRO::wrap);
                Map<String, String> headers0 = new LinkedHashMap<>();
                beginEx.headers().forEach(h -> headers0.put(h.name().asString(), h.value().asString()));
                headers = headers0;
            }
            final Optional<Route> optional = resolveTarget(sourceRef, headers);

            if (optional.isPresent())
            {
                slotIndex = slab.acquire(sourceId);
                if (slotIndex == NO_SLOT)
                {
                    source.doReset(sourceId);
                    this.streamState = this::streamAfterReplyOrReset;
                }
                else
                {
                    byte[] bytes = encodeHeaders(headers, buffer, index, length);
                    slotPosition = 0;
                    MutableDirectBuffer slot = slab.buffer(slotIndex);
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
                    pseudoHeaders[METHOD] = value;
                    switch(value.toLowerCase())
                    {
                    case "post":
                    case "insert":
                        this.persistent = false;
                    }
                    break;
                case ":scheme":
                    pseudoHeaders[SCHEME] = value;
                    break;
                case ":authority":
                    pseudoHeaders[AUTHORITY] = value;
                    break;
                case ":path":
                    pseudoHeaders[PATH] = value;
                    break;
                case "host":
                    if (pseudoHeaders[AUTHORITY] == null)
                    {
                        pseudoHeaders[AUTHORITY] = value;
                    }
                    else if (!pseudoHeaders[AUTHORITY].equals(value))
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

            if (pseudoHeaders[METHOD] == null || pseudoHeaders[SCHEME] == null || pseudoHeaders[PATH] == null
                    || pseudoHeaders[AUTHORITY] == null)
            {
                processUnexpected(buffer, index, length);
            }

            String payloadChars =
                    new StringBuilder().append(pseudoHeaders[METHOD]).append(" ").append(pseudoHeaders[PATH])
                                       .append(" HTTP/1.1").append("\r\n")
                                       .append("Host").append(": ").append(pseudoHeaders[AUTHORITY]).append("\r\n")
                                       .append(headersChars).append("\r\n").toString();
            return payloadChars.getBytes(StandardCharsets.US_ASCII);
        }

        private ConnectionPool getConnectionPool(final Target target, long targetRef)
        {
            Map<Long, ConnectionPool> connectionsByRef = connectionPools.
                    computeIfAbsent(target.name(), (n) -> new Long2ObjectHashMap<ConnectionPool>());
            return connectionsByRef.computeIfAbsent(targetRef, (r) ->
                new ConnectionPool(maximumConnectionsPerRoute, supplyTargetId, supplyTarget,
                        correlateEstablished, target, targetRef));
        }

        private void processData(
            DirectBuffer buffer,
            int index,
            int length)
        {
            dataRO.wrap(buffer, index, index + length);

            sourceWindow -= dataRO.length();
            if (sourceWindow < 0)
            {
                processUnexpected(buffer, index, length);
            }
            else
            {
                final OctetsFW payload = dataRO.payload();
                target.doData(connection.outputStreamId, payload);
                connection.window -= payload.sizeof();
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
            target.removeThrottle(connection.outputStreamId);

            source.removeStream(sourceId);
            this.streamState = this::streamAfterEnd;
        }

        private void processUnexpected(
            DirectBuffer buffer,
            int index,
            int length)
        {
            frameRO.wrap(buffer, index, index + length);

            final long streamId = frameRO.streamId();

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
                windowRO.wrap(buffer, index, index + length);
                connection.window += windowRO.update();
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
                windowRO.wrap(buffer, index, index + length);
                int update = windowRO.update();
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
            MutableDirectBuffer slot = slab.buffer(slotIndex);
            target.doData(connection.outputStreamId, slot, slotOffset, writableBytes);
            connection.window -= writableBytes;
            slotOffset += writableBytes;
            int bytesDeferred = slotPosition - slotOffset;
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
            resetRO.wrap(buffer, index, index + length);
            slab.release(slotIndex);
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
}
