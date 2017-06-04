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
package org.reaktivity.nukleus.http.internal.conductor;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.broadcast.BroadcastTransmitter;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.Reaktive;
import org.reaktivity.nukleus.http.internal.Context;
import org.reaktivity.nukleus.http.internal.router.Router;
import org.reaktivity.nukleus.http.internal.types.OctetsFW;
import org.reaktivity.nukleus.http.internal.types.control.ErrorFW;
import org.reaktivity.nukleus.http.internal.types.control.HttpRouteExFW;
import org.reaktivity.nukleus.http.internal.types.control.Role;
import org.reaktivity.nukleus.http.internal.types.control.RouteFW;
import org.reaktivity.nukleus.http.internal.types.control.RoutedFW;
import org.reaktivity.nukleus.http.internal.types.control.UnrouteFW;
import org.reaktivity.nukleus.http.internal.types.control.UnroutedFW;

@Reaktive
public final class Conductor implements Nukleus
{
    private static final Map<String, String> EMPTY_HEADERS = Collections.emptyMap();

    private final RouteFW routeRO = new RouteFW();
    private final UnrouteFW unrouteRO = new UnrouteFW();

    private final HttpRouteExFW httpRouteExRO = new HttpRouteExFW();

    private final ErrorFW.Builder errorRW = new ErrorFW.Builder();
    private final RoutedFW.Builder routedRW = new RoutedFW.Builder();
    private final UnroutedFW.Builder unroutedRW = new UnroutedFW.Builder();

    private final RingBuffer conductorCommands;
    private final BroadcastTransmitter conductorResponses;
    private final AtomicBuffer sendBuffer;

    private Router router;

    public Conductor(
        Context context)
    {
        this.conductorCommands = context.conductorCommands();
        this.conductorResponses = context.conductorResponses();
        this.sendBuffer = new UnsafeBuffer(new byte[context.maxControlResponseLength()]);
    }

    public void setRouter(
        Router router)
    {
        this.router = router;
    }

    @Override
    public int process()
    {
        return conductorCommands.read(this::handleCommand);
    }

    @Override
    public String name()
    {
        return "conductor";
    }

    public void onErrorResponse(long correlationId)
    {
        ErrorFW errorRO = errorRW.wrap(sendBuffer, 0, sendBuffer.capacity())
                                 .correlationId(correlationId)
                                 .build();

        conductorResponses.transmit(errorRO.typeId(), errorRO.buffer(), errorRO.offset(), errorRO.sizeof());
    }

    public void onRoutedResponse(
        long correlationId,
        long sourceRef)
    {
        RoutedFW routedRO = routedRW.wrap(sendBuffer, 0, sendBuffer.capacity())
                                    .correlationId(correlationId)
                                    .sourceRef(sourceRef)
                                    .build();

        conductorResponses.transmit(routedRO.typeId(), routedRO.buffer(), routedRO.offset(), routedRO.sizeof());
    }

    public void onUnroutedResponse(
        long correlationId)
    {
        UnroutedFW unroutedRO = unroutedRW.wrap(sendBuffer, 0, sendBuffer.capacity())
                                          .correlationId(correlationId)
                                          .build();

        conductorResponses.transmit(unroutedRO.typeId(), unroutedRO.buffer(), unroutedRO.offset(), unroutedRO.sizeof());
    }

    private void handleCommand(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case RouteFW.TYPE_ID:
            handleRouteCommand(buffer, index, length);
            break;
        case UnrouteFW.TYPE_ID:
            handleUnrouteCommand(buffer, index, length);
            break;
        default:
            // ignore unrecognized commands (forwards compatible)
            break;
        }
    }

    private void handleRouteCommand(
        DirectBuffer buffer,
        int index,
        int length)
    {
        final RouteFW route = routeRO.wrap(buffer, index, index + length);

        final long correlationId = route.correlationId();
        final Role role = route.role().get();
        final String source = route.source().asString();
        final long sourceRef = route.sourceRef();
        final String target = route.target().asString();
        final long targetRef = route.targetRef();
        final OctetsFW extension = route.extension();

        router.doRoute(correlationId, role, source, sourceRef, target, targetRef, headers(extension));
    }

    private void handleUnrouteCommand(DirectBuffer buffer, int index, int length)
    {
        final UnrouteFW unroute = unrouteRO.wrap(buffer, index, index + length);

        final long correlationId = unroute.correlationId();
        final Role role = unroute.role().get();
        final String source = unroute.source().asString();
        final long sourceRef = unroute.sourceRef();
        final String target = unroute.target().asString();
        final long targetRef = unroute.targetRef();
        final OctetsFW extension = unroute.extension();

        router.doUnroute(correlationId, role, source, sourceRef, target, targetRef, headers(extension));
    }

    private Map<String, String> headers(
        OctetsFW extension)
    {
        if (extension.sizeof() == 0)
        {
            return EMPTY_HEADERS;
        }
        else
        {
            final HttpRouteExFW routeEx = extension.get(httpRouteExRO::wrap);
            final Map<String, String> headers = new LinkedHashMap<>();
            routeEx.headers().forEach(h -> headers.put(h.name().asString(), h.value().asString()));

            return headers;
        }
    }
}
