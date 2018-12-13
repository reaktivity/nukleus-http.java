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

import java.util.function.Consumer;

import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.route.RouteManager;

/**
 * This class represents state shared between the server accept (source input) and server accept reply
 * (source output established) streams.
 */
final class ServerAcceptState
{
    final String acceptReplyName;
    final long acceptRouteId;
    final long replyStreamId;
    final MessageConsumer acceptReply;
    private final MessageConsumer initialThrottle;
    final Consumer<MessageConsumer> setThrottle;
    final Consumer<Runnable> setCleanupConnectReply;

    int acceptReplyBudget;
    int acceptReplyPadding;
    int pendingRequests;
    boolean endRequested;
    boolean persistent = true;

    ServerAcceptState(
        String acceptReplyName,
        long acceptRouteId,
        long replyStreamId,
        MessageConsumer acceptReply,
        MessageWriter writer,
        MessageConsumer initialThrottle,
        RouteManager router,
        Consumer<Runnable> setCleanupConnectReply)
    {
        this.acceptRouteId = acceptRouteId;
        this.replyStreamId = replyStreamId;
        this.acceptReply = acceptReply;
        this.initialThrottle = initialThrottle;
        this.acceptReplyName = acceptReplyName;
        this.setThrottle = (t) -> router.setThrottle(acceptReplyName, replyStreamId, t);
        this.setCleanupConnectReply = setCleanupConnectReply;
        setThrottle.accept(initialThrottle);
    }

    @Override
    public String toString()
    {
        return String.format(
                "%s[streamId=%016x, target=%s, window=%d, padding=%d persistent=%b, pendingRequests=%d, endRequested=%b]",
                getClass().getSimpleName(), replyStreamId, acceptReplyName, acceptReplyBudget,
                acceptReplyPadding, persistent, pendingRequests, endRequested);
    }

    public void restoreInitialThrottle()
    {
        setThrottle.accept(initialThrottle);
    }

    public void doEnd(MessageWriter writer, long traceId)
    {
        if (pendingRequests == 0)
        {
            writer.doEnd(acceptReply, acceptRouteId, replyStreamId, traceId);
            // TODO: unset throttle on acceptReply
        }
        else
        {
            endRequested = true;
        }
    }

    public void doAbort(MessageWriter writer, long traceId)
    {
        writer.doAbort(acceptReply, acceptRouteId, replyStreamId, traceId);
    }
}


