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

import static java.util.Collections.emptyList;
import static org.reaktivity.nukleus.http.internal.InternalSystemProperty.MAXIMUM_HEADERS_SIZE;
import static org.reaktivity.nukleus.http.internal.InternalSystemProperty.MEMORY_FOR_DECODE;
import static org.reaktivity.nukleus.http.internal.routable.Route.headersMatch;
import static org.reaktivity.nukleus.http.internal.routable.Route.sourceMatches;
import static org.reaktivity.nukleus.http.internal.routable.Route.sourceRefMatches;
import static org.reaktivity.nukleus.http.internal.routable.Route.targetMatches;
import static org.reaktivity.nukleus.http.internal.routable.Route.targetRefMatches;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

import org.agrona.LangUtil;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.Reaktive;
import org.reaktivity.nukleus.http.internal.Context;
import org.reaktivity.nukleus.http.internal.conductor.Conductor;
import org.reaktivity.nukleus.http.internal.layouts.StreamsLayout;
import org.reaktivity.nukleus.http.internal.util.function.LongObjectBiConsumer;

@Reaktive
public final class Routable extends Nukleus.Composite
{
    private static final List<Route> EMPTY_ROUTES = emptyList();

    private final Context context;
    private final String sourceName;
    private final Conductor conductor;
    private final AtomicBuffer writeBuffer;
    private final Map<String, Source> sourcesByPartitionName;
    private final Map<String, Target> targetsByName;
    private final Long2ObjectHashMap<List<Route>> routesByRef;
    private final LongObjectBiConsumer<Correlation> correlateNew;
    private final LongFunction<Correlation> correlateEstablished;
    private final LongFunction<Correlation> lookupEstablished;
    private final LongSupplier supplyTargetId;

    public Routable(
        Context context,
        Conductor conductor,
        String sourceName,
        LongObjectBiConsumer<Correlation> correlateNew,
        LongFunction<Correlation> correlateEstablished,
        LongFunction<Correlation> lookupEstablished)
    {
        this.context = context;
        this.conductor = conductor;
        this.sourceName = sourceName;
        this.correlateNew = correlateNew;
        this.correlateEstablished = correlateEstablished;
        this.lookupEstablished = lookupEstablished;
        this.writeBuffer = new UnsafeBuffer(new byte[context.maxMessageLength()]);
        this.sourcesByPartitionName = new HashMap<>();
        this.targetsByName = new HashMap<>();
        this.routesByRef = new Long2ObjectHashMap<>();
        this.supplyTargetId = context.counters().streamsSourced()::increment;
    }

    @Override
    public String name()
    {
        return sourceName;
    }

    public void onReadable(
        String partitionName)
    {
        sourcesByPartitionName.computeIfAbsent(partitionName, this::newSource);
    }

    public void doRoute(
        long correlationId,
        long sourceRef,
        String targetName,
        long targetRef,
        Map<String, String> headers)
    {
        try
        {
            final Target target = supplyTarget(targetName);
            final Route newRoute = new Route(sourceName, sourceRef, target, targetRef, headers);

            routesByRef.computeIfAbsent(sourceRef, this::newRoutes)
                       .add(newRoute);

            conductor.onRoutedResponse(correlationId, sourceRef);
        }
        catch (Exception ex)
        {
            conductor.onErrorResponse(correlationId);
            LangUtil.rethrowUnchecked(ex);
        }
    }

    public void doUnroute(
        long correlationId,
        long sourceRef,
        String targetName,
        long targetRef,
        Map<String, String> headers)
    {
        final List<Route> routes = supplyRoutes(sourceRef);

        final Predicate<Route> filter =
                sourceMatches(sourceName)
                 .and(sourceRefMatches(sourceRef))
                 .and(targetMatches(targetName))
                 .and(targetRefMatches(targetRef))
                 .and(headersMatch(headers));

        if (routes.removeIf(filter))
        {
            conductor.onUnroutedResponse(correlationId);
        }
        else
        {
            conductor.onErrorResponse(correlationId);
        }
    }

    private List<Route> newRoutes(
        long sourceRef)
    {
        return new ArrayList<>();
    }

    private List<Route> supplyRoutes(
        long referenceId)
    {
        return routesByRef.getOrDefault(referenceId, EMPTY_ROUTES);
    }

    private Source newSource(
        String partitionName)
    {
        StreamsLayout layout = new StreamsLayout.Builder()
            .path(context.sourceStreamsPath().apply(partitionName))
            .streamsCapacity(context.streamsBufferCapacity())
            .throttleCapacity(context.throttleBufferCapacity())
            .readonly(true)
            .build();

        int maximumHeadersSize = MAXIMUM_HEADERS_SIZE.intValue();
        int memoryForDecode = MEMORY_FOR_DECODE.intValue(() ->
        {
            int maxStreamsWithIncompleteRequest = context.maximumStreamsCount() < 1024 ? 64
                    : context.maximumStreamsCount() / 16;
            return maximumHeadersSize * maxStreamsWithIncompleteRequest;
        });

        return include(new Source(sourceName, partitionName, layout, writeBuffer,
                                  this::supplyRoutes, supplyTargetId,
                                  this::supplyTarget, correlateNew, lookupEstablished,
                                  correlateEstablished, maximumHeadersSize, memoryForDecode));
    }

    private Target supplyTarget(
        String targetName)
    {
        return targetsByName.computeIfAbsent(targetName, this::newTarget);
    }

    private Target newTarget(
        String targetName)
    {
        StreamsLayout layout = new StreamsLayout.Builder()
                .path(context.targetStreamsPath().apply(sourceName, targetName))
                .streamsCapacity(context.streamsBufferCapacity())
                .throttleCapacity(context.throttleBufferCapacity())
                .readonly(false)
                .build();

        return include(new Target(targetName, layout, writeBuffer));
    }
}
