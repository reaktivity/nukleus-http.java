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

import java.util.ArrayDeque;
import java.util.Deque;
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
        END, ABORT;
    };
    private final Deque<Connection> availableConnections;
    private final String connectName;
    private final long connectRef;
    private final ClientStreamFactory factory;

    private int connectionsInUse;
    private ConnectionRequest nextRequest;

    ConnectionPool(ClientStreamFactory factory, String connectName, long connectRef)
    {
        this.factory = factory;
        this.connectName = connectName;
        this.connectRef = connectRef;
        this.availableConnections = new ArrayDeque<Connection>(factory.maximumConnectionsPerRoute);
    }

    public void acquire(ConnectionRequest request)
    {
        Connection connection = availableConnections.poll();
        if (connection == null && connectionsInUse < factory.maximumConnectionsPerRoute)
        {
            connection = newConnection();
        }
        if (connection != null)
        {
            request.getConsumer().accept(connection);
        }
        else
        {
            enqueue(request);
        }
    }

    public void cancel(ConnectionRequest request)
    {
        ConnectionRequest candidate = nextRequest;
        ConnectionRequest prior = null;
        while (request != candidate)
        {
            assert candidate != null;
            prior = candidate;
            candidate = candidate.next();
        }
        if (prior == null)
        {
            nextRequest = null;
        }
        else
        {
            prior.next(candidate.next());
        }
        factory.incrementDequeues.getAsLong();
    }

    private Connection newConnection()
    {
        final long correlationId = factory.supplyCorrelationId.getAsLong();
        final long streamId = factory.supplyStreamId.getAsLong();
        Connection connection = new Connection(streamId, correlationId);
        MessageConsumer output = factory.router.supplyTarget(connectName);
        factory.writer.doBegin(output, streamId, connectRef, correlationId);
        factory.router.setThrottle(connectName, streamId, connection::handleThrottleDefault);
        connectionsInUse++;
        return connection;
    }

    public void release(Connection connection)
    {
        release(connection, null);
    }

    public void release(Connection connection, CloseAction action)
    {
        final Correlation<?> correlation = factory.correlations.remove(connection.correlationId);
        if (correlation != null)
        {
            // We did not yet send response headers (high level begin) to the client accept reply stream.
            // This implies we got an incomplete response. We report this as service unavailable (503).
            MessageConsumer acceptReply = factory.router.supplyTarget(correlation.source());
            long targetId = factory.supplyStreamId.getAsLong();
            long sourceCorrelationId = correlation.id();
            factory.writer.doHttpBegin(acceptReply, targetId, 0L, sourceCorrelationId,
                    hs -> hs.item(h -> h.representation((byte) 0).name(":status").value("503")));
            factory.writer.doHttpEnd(acceptReply, targetId);
        }
        if (connection.persistent)
        {
            setDefaultThrottle(connection);
            availableConnections.add(connection);
        }
        else
        {
            connectionsInUse--;

            // In case the connection was previously released when it was still persistent
            availableConnections.removeFirstOccurrence(connection);

            if (action != null && !connection.endOrAbortSent)
            {
                MessageConsumer connect = factory.router.supplyTarget(connectName);
                switch(action)
                {
                case END:
                    factory.writer.doEnd(connect, connection.connectStreamId);
                    break;
                case ABORT:
                    factory.writer.doAbort(connect, connection.connectStreamId);
                }
                connection.endOrAbortSent = true;
            }
        }
        if (nextRequest != null)
        {
            ConnectionRequest current = nextRequest;
            nextRequest = nextRequest.next();
            factory.incrementDequeues.getAsLong();
            acquire(current);
        }
    }

    public void setDefaultThrottle(Connection connection)
    {
        factory.router.setThrottle(connectName, connection.connectStreamId, connection::handleThrottleDefault);
    }

    private void enqueue(ConnectionRequest request)
    {
        if (this.nextRequest == null)
        {
            this.nextRequest = request;
        }
        else
        {
            ConnectionRequest latest = this.nextRequest;
            while (latest.next() != null)
            {
                latest = latest.next();
            }
            latest.next(request);
        }
        factory.incrementEnqueues.getAsLong();
    }

    public interface ConnectionRequest
    {
        Consumer<Connection> getConsumer();

        void next(ConnectionRequest next);

        ConnectionRequest next();
    }

    public class Connection
    {
        final long connectStreamId;
        final long correlationId;
        int budget;
        int padding;
        boolean persistent = true;
        private boolean endOrAbortSent;

        private long connectReplyStreamId;
        private MessageConsumer connectReplyThrottle;

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
                    factory.writer.doReset(connectReplyThrottle, connectReplyStreamId);
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

