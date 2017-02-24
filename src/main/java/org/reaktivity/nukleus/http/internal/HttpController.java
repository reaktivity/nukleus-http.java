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
package org.reaktivity.nukleus.http.internal;

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.nativeOrder;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.broadcast.BroadcastReceiver;
import org.agrona.concurrent.broadcast.CopyBroadcastReceiver;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.reaktivity.nukleus.Controller;
import org.reaktivity.nukleus.http.internal.types.OctetsFW;
import org.reaktivity.nukleus.http.internal.types.control.ErrorFW;
import org.reaktivity.nukleus.http.internal.types.control.HttpRouteExFW;
import org.reaktivity.nukleus.http.internal.types.control.Role;
import org.reaktivity.nukleus.http.internal.types.control.RouteFW;
import org.reaktivity.nukleus.http.internal.types.control.RoutedFW;
import org.reaktivity.nukleus.http.internal.types.control.State;
import org.reaktivity.nukleus.http.internal.types.control.UnrouteFW;
import org.reaktivity.nukleus.http.internal.types.control.UnroutedFW;

public final class HttpController implements Controller
{
    private static final int MAX_SEND_LENGTH = 1024; // TODO: Configuration and Context

    // TODO: thread-safe flyweights or command queue from public methods
    private final RouteFW.Builder routeRW = new RouteFW.Builder();
    private final UnrouteFW.Builder unrouteRW = new UnrouteFW.Builder();

    private final HttpRouteExFW.Builder routeExRW = new HttpRouteExFW.Builder();

    private final ErrorFW errorRO = new ErrorFW();
    private final RoutedFW routedRO = new RoutedFW();
    private final UnroutedFW unroutedRO = new UnroutedFW();

    private final Context context;
    private final RingBuffer conductorCommands;
    private final CopyBroadcastReceiver conductorResponses;
    private final AtomicBuffer atomicBuffer;
    private final Long2ObjectHashMap<CompletableFuture<?>> promisesByCorrelationId;

    public HttpController(Context context)
    {
        this.context = context;
        this.conductorCommands = context.conductorCommands();
        this.conductorResponses = new CopyBroadcastReceiver(new BroadcastReceiver(context.conductorResponseBuffer()));
        this.atomicBuffer = new UnsafeBuffer(allocateDirect(MAX_SEND_LENGTH).order(nativeOrder()));
        this.promisesByCorrelationId = new Long2ObjectHashMap<>();
    }

    @Override
    public int process()
    {
        int weight = 0;

        weight += conductorResponses.receive(this::handleResponse);

        return weight;
    }

    @Override
    public void close() throws Exception
    {
        context.close();
    }

    @Override
    public Class<HttpController> kind()
    {
        return HttpController.class;
    }

    @Override
    public String name()
    {
        return "http";
    }

    public CompletableFuture<Long> routeInputNone(
        String source,
        long sourceRef,
        String target,
        long targetRef,
        Map<String, String> headers)
    {
        return route(Role.INPUT, State.NONE, source, sourceRef, target, targetRef, headers);
    }

    public CompletableFuture<Long> routeInputEstablished(
        String source,
        long sourceRef,
        String target,
        long targetRef,
        Map<String, String> headers)
    {
        return route(Role.INPUT, State.ESTABLISHED, source, sourceRef, target, targetRef, headers);
    }

    public CompletableFuture<Long> routeInputNew(
        String source,
        long sourceRef,
        String target,
        long targetRef,
        Map<String, String> headers)
    {
        return route(Role.INPUT, State.NEW, source, sourceRef, target, targetRef, headers);
    }

    public CompletableFuture<Long> routeOutputNone(
        String source,
        long sourceRef,
        String target,
        long targetRef,
        Map<String, String> headers)
    {
        return route(Role.OUTPUT, State.NONE, source, sourceRef, target, targetRef, headers);
    }

    public CompletableFuture<Long> routeOutputEstablished(
        String source,
        long sourceRef,
        String target,
        long targetRef,
        Map<String, String> headers)
    {
        return route(Role.OUTPUT, State.ESTABLISHED, source, sourceRef, target, targetRef, headers);
    }

    public CompletableFuture<Long> routeOutputNew(
        String source,
        long sourceRef,
        String target,
        long targetRef,
        Map<String, String> headers)
    {
        return route(Role.OUTPUT, State.NEW, source, sourceRef, target, targetRef, headers);
    }

    public CompletableFuture<Void> unrouteInputEstablished(
        String source,
        long sourceRef,
        String target,
        long targetRef,
        Map<String, String> headers)
    {
        return unroute(Role.INPUT, State.ESTABLISHED, source, sourceRef, target, targetRef, headers);
    }

    public CompletableFuture<Void> unrouteInputNew(
        String source,
        long sourceRef,
        String target,
        long targetRef,
        Map<String, String> headers)
    {
        return unroute(Role.INPUT, State.NEW, source, sourceRef, target, targetRef, headers);
    }

    public CompletableFuture<Void> unrouteInputNone(
        String source,
        long sourceRef,
        String target,
        long targetRef,
        Map<String, String> headers)
    {
        return unroute(Role.INPUT, State.NONE, source, sourceRef, target, targetRef, headers);
    }

    public CompletableFuture<Void> unrouteOutputEstablished(
        String source,
        long sourceRef,
        String target,
        long targetRef,
        Map<String, String> headers)
    {
        return unroute(Role.OUTPUT, State.ESTABLISHED, source, sourceRef, target, targetRef, headers);
    }

    public CompletableFuture<Void> unrouteOutputNew(
        String source,
        long sourceRef,
        String target,
        long targetRef,
        Map<String, String> headers)
    {
        return unroute(Role.OUTPUT, State.NEW, source, sourceRef, target, targetRef, headers);
    }

    public CompletableFuture<Void> unrouteOutputNone(
        String source,
        long sourceRef,
        String target,
        long targetRef,
        Map<String, String> headers)
    {
        return unroute(Role.OUTPUT, State.NONE, source, sourceRef, target, targetRef, headers);
    }


    public HttpStreams streams(
        String source)
    {
        int streamsCapacity = context.streamsBufferCapacity();
        int throttleCapacity = context.throttleBufferCapacity();
        Path path = context.sourceStreamsPath().apply(source);

        return new HttpStreams(streamsCapacity, throttleCapacity, path, false);
    }

    public HttpStreams streams(
        String source,
        String target)
    {
        int streamsCapacity = context.streamsBufferCapacity();
        int throttleCapacity = context.throttleBufferCapacity();
        Path path = context.targetStreamsPath().apply(source, target);

        return new HttpStreams(streamsCapacity, throttleCapacity, path, true);
    }

    private Consumer<OctetsFW.Builder> extension(
        Map<String, String> headers)
    {
        if (headers != null)
        {
            return e -> e.set((buffer, offset, limit) ->
                routeExRW.wrap(buffer, offset, limit)
                         .headers(hs ->
                         {
                             headers.forEach((k, v) ->
                             {
                                 hs.item(h -> h.name(k).value(v));
                             });
                         })
                         .build()
                         .sizeof());
        }
        else
        {
            return e -> e.reset();
        }
    }

    private int handleResponse(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case ErrorFW.TYPE_ID:
            handleErrorResponse(buffer, index, length);
            break;
        case RoutedFW.TYPE_ID:
            handleRoutedResponse(buffer, index, length);
            break;
        case UnroutedFW.TYPE_ID:
            handleUnroutedResponse(buffer, index, length);
            break;
        default:
            break;
        }

        return 1;
    }

    private void handleErrorResponse(
        DirectBuffer buffer,
        int index,
        int length)
    {
        errorRO.wrap(buffer, index, length);
        long correlationId = errorRO.correlationId();

        CompletableFuture<?> promise = promisesByCorrelationId.remove(correlationId);
        if (promise != null)
        {
            commandFailed(promise, "command failed");
        }
    }


    @SuppressWarnings("unchecked")
    private void handleRoutedResponse(
        DirectBuffer buffer,
        int index,
        int length)
    {
        routedRO.wrap(buffer, index, length);
        long correlationId = routedRO.correlationId();
        long sourceRef = routedRO.sourceRef();

        CompletableFuture<Long> promise = (CompletableFuture<Long>) promisesByCorrelationId.remove(correlationId);
        if (promise != null)
        {
            commandSucceeded(promise, sourceRef);
        }
    }

    private void handleUnroutedResponse(
        DirectBuffer buffer,
        int index,
        int length)
    {
        unroutedRO.wrap(buffer, index, length);
        long correlationId = unroutedRO.correlationId();

        CompletableFuture<?> promise = promisesByCorrelationId.remove(correlationId);
        if (promise != null)
        {
            commandSucceeded(promise);
        }
    }

    private void commandSent(
        final long correlationId,
        final CompletableFuture<?> promise)
    {
        promisesByCorrelationId.put(correlationId, promise);
    }

    private <T> boolean commandSucceeded(
        final CompletableFuture<T> promise)
    {
        return commandSucceeded(promise, null);
    }

    private <T> boolean commandSucceeded(
        final CompletableFuture<T> promise,
        final T value)
    {
        return promise.complete(value);
    }

    private boolean commandSendFailed(
        final CompletableFuture<?> promise)
    {
        return commandFailed(promise, "unable to offer command");
    }

    private boolean commandFailed(
        final CompletableFuture<?> promise,
        final String message)
    {
        return promise.completeExceptionally(new IllegalStateException(message).fillInStackTrace());
    }

    private CompletableFuture<Long> route(
        Role role,
        State state,
        String source,
        long sourceRef,
        String target,
        long targetRef,
        Map<String, String> headers)
    {
        final CompletableFuture<Long> promise = new CompletableFuture<>();

        long correlationId = conductorCommands.nextCorrelationId();

        RouteFW routeRO = routeRW.wrap(atomicBuffer, 0, atomicBuffer.capacity())
                                 .correlationId(correlationId)
                                 .role(b -> b.set(role))
                                 .state(b -> b.set(state))
                                 .source(source)
                                 .sourceRef(sourceRef)
                                 .target(target)
                                 .targetRef(targetRef)
                                 .extension(extension(headers))
                                 .build();

        if (!conductorCommands.write(routeRO.typeId(), routeRO.buffer(), routeRO.offset(), routeRO.sizeof()))
        {
            commandSendFailed(promise);
        }
        else
        {
            commandSent(correlationId, promise);
        }

        return promise;
    }

    private CompletableFuture<Void> unroute(
        Role role,
        State state,
        String source,
        long sourceRef,
        String target,
        long targetRef,
        Map<String, String> headers)
    {
        final CompletableFuture<Void> promise = new CompletableFuture<>();

        long correlationId = conductorCommands.nextCorrelationId();

        UnrouteFW unrouteRO = unrouteRW.wrap(atomicBuffer, 0, atomicBuffer.capacity())
                                 .correlationId(correlationId)
                                 .role(b -> b.set(role))
                                 .state(b -> b.set(state))
                                 .source(source)
                                 .sourceRef(sourceRef)
                                 .target(target)
                                 .targetRef(targetRef)
                                 .extension(extension(headers))
                                 .build();

        if (!conductorCommands.write(unrouteRO.typeId(), unrouteRO.buffer(), unrouteRO.offset(), unrouteRO.sizeof()))
        {
            commandSendFailed(promise);
        }
        else
        {
            commandSent(correlationId, promise);
        }

        return promise;
    }

}
