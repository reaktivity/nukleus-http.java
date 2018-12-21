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

import java.util.Objects;

public class Correlation<S>
{
    private final long routeId;
    private final long replyId;
    private final long id;
    private final S state;

    public Correlation(
        long id,
        long routeId,
        long replyId,
        S state)
    {
        this.id = id;
        this.routeId = routeId;
        this.replyId = replyId;
        this.state = state;
    }

    public Correlation(
        long id,
        long routeId,
        long sourceId)
    {
        this(id, routeId, sourceId, null);
    }

    public long routeId()
    {
        return routeId;
    }

    public long replyId()
    {
        return replyId;
    }

    public long id()
    {
        return id;
    }


    public S state()
    {
        return state;
    }

    @Override
    public int hashCode()
    {
        return Long.hashCode(id);
    }

    @Override
    public boolean equals(
        Object obj)
    {
        if (!(obj instanceof Correlation))
        {
            return false;
        }

        Correlation<?> that = (Correlation<?>) obj;
        return this.id == that.id &&
                Objects.equals(this.state, that.state);
    }

    @Override
    public String toString()
    {
        return String.format("[id=%s, state=%s]", id, state);
    }
}
