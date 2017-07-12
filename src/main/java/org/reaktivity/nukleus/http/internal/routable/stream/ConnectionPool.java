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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;

import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.http.internal.routable.Correlation;
import org.reaktivity.nukleus.http.internal.routable.Source;
import org.reaktivity.nukleus.http.internal.routable.Target;
import org.reaktivity.nukleus.http.internal.types.stream.ResetFW;

/**
 * A set of connections (target streams) to be used to talk to a given target on a given route (targetRef)
 */
final class ConnectionPool
{
    private final int maximumConnections;
    private final LongSupplier supplyTargetId;
    private final Function<String, Target> suppyTarget;
    private final LongFunction<Correlation<?>> correlateEstablished;
    private final Deque<Connection> availableConnections;
    private final Target connect;
    private final long connectRef;

    private int connectionsInUse;
    private ConnectionRequest nextRequest;

    ConnectionPool(int maximumConnections, LongSupplier supplyTargetId, Function<String, Target> supplyTarget,
            LongFunction<Correlation<?>> correlateEstablished, Target target, long targetRef)
    {
        this.maximumConnections = maximumConnections;
        this.availableConnections = new ArrayDeque<Connection>(maximumConnections);
        this.supplyTargetId = supplyTargetId;
        this.suppyTarget = supplyTarget;
        this.correlateEstablished = correlateEstablished;
        this.connect = target;
        this.connectRef = targetRef;
    }

    public void acquire(ConnectionRequest request)
    {
        Connection connection = availableConnections.poll();
        if (connection == null && connectionsInUse < maximumConnections)
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

    private Connection newConnection()
    {
        Connection connection = new Connection(supplyTargetId.getAsLong());
        long targetCorrelationId = connection.outputStreamId;
        connect.doBegin(connection.outputStreamId, connectRef, targetCorrelationId);
        connect.setThrottle(connection.outputStreamId, connection::throttleReleaseOnReset);
        connectionsInUse++;
        return connection;
    }

    public void release(Connection connection, boolean doEndIfNotPersistent)
    {
        @SuppressWarnings("unchecked")
        final Correlation<?> correlation = correlateEstablished.apply(connection.connectCorrelationId);
        if (correlation != null)
        {
            // We did not yet send response headers (high level begin) to the client accept reply stream.
            // This implies we got an incomplete response. We report this as service unavailable (503).
            Target acceptReply = suppyTarget.apply(correlation.source());
            long targetId = supplyTargetId.getAsLong();
            long sourceCorrelationId = correlation.id();
            acceptReply.doHttpBegin(targetId, 0L, sourceCorrelationId,
                    hs -> hs.item(h -> h.representation((byte) 0).name(":status").value("503")));
            acceptReply.doHttpEnd(targetId);

        }
        if (connection.persistent)
        {
            connect.setThrottle(connection.outputStreamId, connection::throttleReleaseOnReset);
            availableConnections.add(connection);
        }
        else
        {
            connectionsInUse--;

            // In case the connection was previously released when it was still persistent
            availableConnections.removeFirstOccurrence(connection);

            if (doEndIfNotPersistent)
            {
                connect.doEnd(connection.outputStreamId);
                connection.endSent = true;
            }
        }
        if (nextRequest != null)
        {
            ConnectionRequest current = nextRequest;
            nextRequest = nextRequest.next();
            acquire(current);
        }
    }

    private void enqueue(ConnectionRequest request)
    {
        if (this.nextRequest == null)
        {
            this.nextRequest = request;
        }
        else if (request != this.nextRequest)
        {
            ConnectionRequest latest = this.nextRequest;
            while (latest.next() != null)
            {
                latest = latest.next();
            }
            latest.next(request);
        }
    }

    public interface ConnectionRequest
    {
        Consumer<Connection> getConsumer();

        void next(ConnectionRequest next);

        ConnectionRequest next();
    }

    public class Connection
    {
        final long outputStreamId;
        int window;
        boolean persistent = true;
        boolean endSent;

        private long connectReplyStreamId;
        private Source connectReply;
        private long connectCorrelationId = -1;

        Connection(long targetStreamId)
        {
            this.outputStreamId = targetStreamId;
        }

        void setInput(Source source, long sourceId, long connectCorrelationId)
        {
            this.connectReply = source;
            this.connectReplyStreamId = sourceId;
            this.connectCorrelationId = connectCorrelationId;
        }

        private void throttleReleaseOnReset(int msgTypeId, MutableDirectBuffer buffer, int index, int length)
        {
            switch (msgTypeId)
            {
            case ResetFW.TYPE_ID:
                persistent = false;
                release(this, false);
                if (connectReply != null)
                {
                    connectReply.doReset(connectReplyStreamId);
                }
                break;
            default:
                // ignore
                break;
            }
        }
    }

}

