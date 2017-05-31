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

import java.util.function.Function;

import org.agrona.concurrent.MessageHandler;
import org.reaktivity.nukleus.http.internal.routable.Target;

/**
 * This class represents state shared between the server accept (source input) and server accept reply
 * (source output established) streams.
 */
final class ServerAcceptState
{
    final long streamId;
    final Target replyTarget;
    private final MessageHandler initialThrottle;
    int window;
    int pendingRequests;
    boolean endRequested;

    ServerAcceptState(long streamId, Target replyTarget, MessageHandler initialThrottle)
    {
        this.streamId = streamId;
        this.replyTarget = replyTarget;
        this.initialThrottle = initialThrottle;
        replyTarget.setThrottle(streamId, initialThrottle);
    }

    @Override
    public String toString()
    {
        return String.format(
                "%s[streamId=%016x, target=%s, window=%d, started=%b, pendingRequests=%d, endRequested=%b]",
                getClass().getSimpleName(), streamId, replyTarget, window, pendingRequests, endRequested);
    }

    public void restoreInitialThrottle()
    {
        replyTarget.setThrottle(streamId, initialThrottle);
    }

    public void doEnd(Function<String, Target> supplyTarget)
    {
        if (pendingRequests == 0)
        {
            replyTarget.doEnd(streamId);
            replyTarget.removeThrottle(streamId);
        }
        else
        {
            endRequested = true;
        }
    }

}


