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
package org.reaktivity.nukleus.http.internal.routable;

import static java.util.Objects.requireNonNull;

import java.util.Objects;

import org.reaktivity.nukleus.http.internal.router.RouteKind;

public class Correlation
{
    private final String sourceName;
    private final long sourceCorrelationId;
    private final RouteKind establishedRouteKind;

    public Correlation(
        String sourceName,
        long sourceCorrelationId,
        RouteKind establishedRouteKind)
    {
        this.sourceName = requireNonNull(sourceName, "sourceName");
        this.sourceCorrelationId = sourceCorrelationId;
        this.establishedRouteKind = establishedRouteKind;
    }

    public String source()
    {
        return sourceName;
    }

    public long sourceCorrelationId()
    {
        return sourceCorrelationId;
    }

    public RouteKind getEstablishedRouteKind()
    {
        return establishedRouteKind;
    }

    @Override
    public int hashCode()
    {
        int result = sourceName.hashCode();
        result = 31 * result + Long.hashCode(sourceCorrelationId);

        return result;
    }

    @Override
    public boolean equals(
        Object obj)
    {
        if (!(obj instanceof Correlation))
        {
            return false;
        }

        Correlation that = (Correlation) obj;
        return this.sourceCorrelationId == that.sourceCorrelationId &&
                Objects.equals(this.sourceName, that.sourceName);
    }

    @Override
    public String toString()
    {
        return String.format("[source=\"%s\", sourceCorrelationId=%s]", sourceName, sourceCorrelationId);
    }
}
