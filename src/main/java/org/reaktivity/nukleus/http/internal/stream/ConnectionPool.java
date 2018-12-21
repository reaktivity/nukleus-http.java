/**
 * Copyright 2016-2018 The Reaktivity Project
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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Queue;
import java.util.function.Consumer;

import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.http.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http.internal.types.stream.WindowFW;

/**
 * A set of connections (target streams) to be used to talk to a given target on a given route (targetRef)
 */
final class ConnectionPool
{
    public enum CloseAction
    {
        END, ABORT
    }
    private final Deque<Connection> availableConnections;
    private final long connectRouteId;
    private final ClientStreamFactory factory;
    private final Queue<ConnectionRequest> queuedRequests;

    private int connectionsInUse;

    ConnectionPool(
        ClientStreamFactory factory,
        long connectRouteId)
    {
        this.factory = factory;
        this.connectRouteId = connectRouteId;
        this.availableConnections = new ArrayDeque<>(factory.maximumConnectionsPerRoute);
        this.queuedRequests = new ArrayDeque<>(factory.maximumQueuedRequestsPerRoute);
    }

    /*
     * @return true if the given request is served immediately or later, otherwise false
     */
    boolean acquire(ConnectionRequest request)
    {
        Connection connection = availableConnections.poll();
        if (connection == null && connectionsInUse < factory.maximumConnectionsPerRoute)
        {
            connection = newConnection();
        }
        if (connection != null)
        {
            connection.noRequests++;
            request.getConsumer().accept(connection);
        }
        else if (queuedRequests.size() < factory.maximumQueuedRequestsPerRoute)
        {
            queuedRequests.add(request);
            factory.enqueues.getAsLong();
        }
        else
        {
            return false;
        }

        return true;
    }

    private void acquireNextIfQueued()
    {
        if (!queuedRequests.isEmpty())
        {
            Connection connection = availableConnections.poll();
            if (connection == null && connectionsInUse < factory.maximumConnectionsPerRoute)
            {
                connection = newConnection();
            }

            if (connection != null)
            {
                ConnectionRequest nextRequest = queuedRequests.poll();
                factory.dequeues.getAsLong();
                nextRequest.getConsumer().accept(connection);
                connection.noRequests++;
            }
        }
    }

    void cancel(ConnectionRequest request)
    {
        queuedRequests.remove(request);
        factory.dequeues.getAsLong();
    }

    private Connection newConnection()
    {
        final long correlationId = factory.supplyCorrelationId.getAsLong();
        final long streamId = factory.supplyInitialId.getAsLong();
        Connection connection = new Connection(streamId, correlationId);
        MessageConsumer output = factory.router.supplyReceiver(connectRouteId);
        factory.writer.doBegin(output, connectRouteId, streamId, factory.supplyTraceId, correlationId);
        factory.router.setThrottle(streamId, connection::handleThrottleDefault);
        connectionsInUse++;
        return connection;
    }

    void release(Connection connection)
    {
        release(connection, null);
    }

    void release(Connection connection, CloseAction action)
    {
        final Correlation<?> correlation = factory.correlations.remove(connection.correlationId);
        if (correlation != null)
        {
            // We did not yet send response headers (high level begin) to the client accept reply stream.
            // This implies we got an incomplete response. We report this as service unavailable (503).
            long acceptRouteId = correlation.routeId();
            MessageConsumer acceptReply = factory.router.supplySender(acceptRouteId);
            long acceptReplyId = correlation.replyId();
            long traceId = factory.supplyTraceId;

            // count abandoned requests
            factory.countRequestsAbandoned.getAsLong();

            // count all responses
            factory.countResponses.getAsLong();

            long acceptCorrelationId = correlation.id();
            factory.writer.doHttpBegin(acceptReply, acceptRouteId, acceptReplyId, traceId, acceptCorrelationId,
                                       hs -> hs.item(h -> h.name(":status").value("503"))
                                               .item(h -> h.name("retry-after").value("0")));
            factory.writer.doHttpEnd(acceptReply, acceptRouteId, acceptReplyId, factory.supplyTrace.getAsLong());
        }
        if (connection.persistent)
        {
            setDefaultThrottle(connection);
            availableConnections.add(connection);
        }
        else
        {
            // release() gets called multiple times for a connection
            if (!connection.released)
            {
                connection.released = true;
                connectionsInUse--;
                assert connectionsInUse >= 0;
            }

            // In case the connection was previously released when it was still persistent
            availableConnections.removeFirstOccurrence(connection);

            if (action != null && !connection.endOrAbortSent)
            {
                MessageConsumer connect = factory.router.supplyReceiver(connectRouteId);
                switch(action)
                {
                case END:
                    factory.writer.doEnd(connect, connectRouteId, connection.connectStreamId, factory.supplyTrace.getAsLong());
                    break;
                case ABORT:
                    factory.writer.doAbort(connect, connectRouteId, connection.connectStreamId, factory.supplyTrace.getAsLong());
                }
                connection.endOrAbortSent = true;
            }
        }

        acquireNextIfQueued();
    }

    void setDefaultThrottle(Connection connection)
    {
        factory.router.setThrottle(connection.connectStreamId, connection::handleThrottleDefault);
    }

    public interface ConnectionRequest
    {
        Consumer<Connection> getConsumer();
    }

    class Connection
    {
        final long connectStreamId;
        final long correlationId;
        int budget;
        int padding;
        boolean persistent = true;
        boolean released;
        private boolean endOrAbortSent;

        private long connectReplyStreamId;
        private MessageConsumer connectReplyThrottle;
        int noRequests;

        Connection(long outputStreamId, long outputCorrelationId)
        {
            this.connectStreamId = outputStreamId;
            this.correlationId = outputCorrelationId;
        }

        void setInput(MessageConsumer connectReplyThrottle, long connectReplyStreamId)
        {
            this.connectReplyThrottle = connectReplyThrottle;
            this.connectReplyStreamId = connectReplyStreamId;
        }

        void handleThrottleDefault(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case ResetFW.TYPE_ID:
                persistent = false;
                release(this);
                if (connectReplyThrottle != null)
                {
                    ResetFW resetFW = factory.resetRO.wrap(buffer, index, index + length);
                    factory.writer.doReset(connectReplyThrottle, connectRouteId, connectReplyStreamId, resetFW.trace());
                }
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = factory.windowRO.wrap(buffer, index, index + length);
                this.budget += window.credit();
                this.padding = window.padding();
                break;
            default:
                // ignore
                break;
            }
        }
    }
}

