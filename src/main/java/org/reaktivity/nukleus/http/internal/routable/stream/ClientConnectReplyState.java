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

import org.reaktivity.nukleus.http.internal.routable.stream.ConnectionPool.Connection;

final class ClientConnectReplyState
{
    private final ConnectionPool connectionPool;
    final Connection connection;
    int pendingResponses;

    ClientConnectReplyState(ConnectionPool connectionPool,
                            Connection connection)
    {
       this.connectionPool = connectionPool;
       this.connection = connection;
       this.pendingResponses = 1;
    }

    @Override
    public String toString()
    {
        return String.format(
                "%s[streamId=%016x, window=%d, pendingResponses=%d]",
                getClass().getSimpleName(), connection.targetId, connection.window, pendingResponses);
    }

    public void releaseConnection(boolean upgraded)
    {
        connectionPool.release(connection, upgraded);
    }

}

