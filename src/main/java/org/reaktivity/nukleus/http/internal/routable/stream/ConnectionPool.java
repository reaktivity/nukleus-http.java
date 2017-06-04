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
import java.util.Queue;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import org.reaktivity.nukleus.http.internal.routable.Target;

/**
 * A set of connections (target streams) to be used to talk to a given target on a given route (targetRef)
 */
final class ConnectionPool
{
    private final int maximumConnections;
    private final LongSupplier supplyTargetId;
    private final Queue<Connection> availableConnections;
    private final Target target;
    private final long targetRef;

    private int connectionsInUse;
    private ConnectionRequest nextRequest;

    ConnectionPool(int maximumConnections, LongSupplier supplyTargetId, Target target, long targetRef)
    {
        this.maximumConnections = maximumConnections;
        this.availableConnections = new ArrayDeque<Connection>(maximumConnections);
        this.supplyTargetId = supplyTargetId;
        this.target = target;
        this.targetRef = targetRef;
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
        Connection connection = new Connection();
        connection.targetId = supplyTargetId.getAsLong();
        long targetCorrelationId = connection.targetId;
        target.doBegin(connection.targetId, targetRef, targetCorrelationId);
        connectionsInUse++;
        return connection;
    }

    public void release(Connection connection, boolean upgraded)
    {
        if (connection.persistent)
        {
            availableConnections.add(connection);
        }
        else
        {
            if (!upgraded)
            {
                target.doEnd(connection.targetId);
            }
            connectionsInUse--;
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
        int window;
        long targetId;
        boolean persistent = true;
    }

}

