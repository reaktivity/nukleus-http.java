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

import org.reaktivity.nukleus.http.internal.routable.Target;

final class ServerAcceptReplyState
{
    final long streamId;
    Target target;
    int window;
    boolean started;
    int pendingRequests;
    public boolean endRequested;

    ServerAcceptReplyState(long streamId, Target target)
    {
        this.streamId = streamId;
    }

    @Override
    public String toString()
    {
        return String.format(
                "[streamId=%016x, target=%s, window=%d, started=%b, pendingRequests=%d, endRequested=%b]",
                getClass().getSimpleName(), streamId, target, window, started, pendingRequests, endRequested);
    }

    public void doEnd(Function<String, Target> supplyTarget)
    {
        if (pendingRequests == 0)
        {
            target.doEnd(streamId);
            // TODO: call a callback registered by TargetOutputEstablishedStream so it can clean up resources
        }
        else
        {
            endRequested = true;
        }
    }
}

