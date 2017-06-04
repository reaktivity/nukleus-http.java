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
package org.reaktivity.nukleus.http.internal.router;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.status.AtomicCounter;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.Reaktive;
import org.reaktivity.nukleus.http.internal.Context;
import org.reaktivity.nukleus.http.internal.conductor.Conductor;
import org.reaktivity.nukleus.http.internal.routable.Correlation;
import org.reaktivity.nukleus.http.internal.routable.Routable;
import org.reaktivity.nukleus.http.internal.types.control.Role;

@Reaktive
public final class Router extends Nukleus.Composite
{
    private static final Pattern SOURCE_NAME = Pattern.compile("([^#]+).*");

    private final Context context;
    private final Map<String, Routable> routables;
    private final Long2ObjectHashMap<Correlation<?>> correlations;
    private final AtomicCounter routesSourced;

    private Conductor conductor;

    public Router(
        Context context)
    {
        this.context = context;
        this.routables = new HashMap<>();
        this.correlations = new Long2ObjectHashMap<>();
        this.routesSourced = context.counters().routesSourced();
    }

    public void setConductor(Conductor conductor)
    {
        this.conductor = conductor;
    }

    @Override
    public String name()
    {
        return "router";
    }

    public void doRoute(
        long correlationId,
        Role role,
        String sourceName,
        long sourceRef,
        String targetName,
        long targetRef,
        Map<String, String> headers)
    {
        final RouteKind routeKind = RouteKind.valueOf(role);

        if (sourceRef == 0L)
        {
            sourceRef = routeKind.nextRef(routesSourced);
        }

        if (RouteKind.match(sourceRef) == routeKind)
        {
            Routable routable = routables.computeIfAbsent(sourceName, this::newRoutable);
            routable.doRoute(correlationId, sourceRef, targetName, targetRef, headers);
        }
        else
        {
            conductor.onErrorResponse(correlationId);
        }
    }

    public void doUnroute(
        long correlationId,
        Role role,
        String sourceName,
        long sourceRef,
        String targetName,
        long targetRef,
        Map<String, String> headers)
    {
        final Routable routable = routables.get(sourceName);
        if (routable != null)
        {
            routable.doUnroute(correlationId, sourceRef, targetName, targetRef, headers);
        }
        else
        {
            conductor.onErrorResponse(correlationId);
        }
    }

    public void onReadable(
        Path sourcePath)
    {
        String sourceName = source(sourcePath);
        Routable routable = routables.computeIfAbsent(sourceName, this::newRoutable);
        String partitionName = sourcePath.getFileName().toString();
        routable.onReadable(partitionName);
    }

    public void onExpired(
        Path sourcePath)
    {
        // TODO:
    }

    private static String source(
        Path path)
    {
        Matcher matcher = SOURCE_NAME.matcher(path.getName(path.getNameCount() - 1).toString());
        if (matcher.matches())
        {
            return matcher.group(1);
        }
        else
        {
            throw new IllegalStateException();
        }
    }

    private Routable newRoutable(
        String sourceName)
    {
        return include(new Routable(context, conductor, sourceName,
                                    correlations::put, correlations::get, correlations::remove));
    }
}
