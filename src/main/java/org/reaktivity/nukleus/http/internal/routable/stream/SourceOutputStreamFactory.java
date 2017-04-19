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

import static java.lang.Character.toUpperCase;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.reaktivity.nukleus.http.internal.routable.Route.headersMatch;
import static org.reaktivity.nukleus.http.internal.routable.stream.Slab.NO_SLOT;
import static org.reaktivity.nukleus.http.internal.router.RouteKind.INPUT_ESTABLISHED;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.MessageHandler;
import org.reaktivity.nukleus.http.internal.routable.Correlation;
import org.reaktivity.nukleus.http.internal.routable.Route;
import org.reaktivity.nukleus.http.internal.routable.Source;
import org.reaktivity.nukleus.http.internal.routable.Target;
import org.reaktivity.nukleus.http.internal.types.OctetsFW;
import org.reaktivity.nukleus.http.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http.internal.types.stream.EndFW;
import org.reaktivity.nukleus.http.internal.types.stream.FrameFW;
import org.reaktivity.nukleus.http.internal.types.stream.HttpBeginExFW;
import org.reaktivity.nukleus.http.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.http.internal.util.function.LongObjectBiConsumer;

public final class SourceOutputStreamFactory
{
    private static final Map<String, String> EMPTY_HEADERS = Collections.emptyMap();

    // Pseudo-headers
    private static final int METHOD = 0;
    private static final int SCHEME = 1;
    private static final int AUTHORITY = 2;
    private static final int PATH = 3;

    private final FrameFW frameRO = new FrameFW();

    private final BeginFW beginRO = new BeginFW();
    private final HttpBeginExFW beginExRO = new HttpBeginExFW();

    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();

    private final WindowFW windowRO = new WindowFW();
    private final ResetFW resetRO = new ResetFW();

    private final Source source;
    private final LongSupplier supplyTargetId;
    private final LongFunction<List<Route>> supplyRoutes;
    private final LongObjectBiConsumer<Correlation> correlateNew;

    private final Slab slab;

    public SourceOutputStreamFactory(
        Source source,
        LongFunction<List<Route>> supplyRoutes,
        LongSupplier supplyTargetId,
        LongObjectBiConsumer<Correlation> correlateNew,
        Slab slab)
    {
        this.source = source;
        this.supplyTargetId = supplyTargetId;
        this.supplyRoutes = supplyRoutes;
        this.correlateNew = correlateNew;
        this.slab = slab;
    }

    public MessageHandler newStream()
    {
        return new SourceOutputStream()::handleStream;
    }

    private final class SourceOutputStream
    {
        private MessageHandler streamState;
        private MessageHandler throttleState;

        private long sourceId;
        private long sourceRef;
        private long correlationId;
        private Target target;
        private long targetId;
        private int window;
        private int slotIndex;
        private int slotPosition;
        private int slotOffset;
        private boolean endDeferred;

        private SourceOutputStream()
        {
            this.streamState = this::streamBeforeBegin;
            this.throttleState = this::throttleBeforeBegin;
        }

        private void handleStream(
            int msgTypeId,
            MutableDirectBuffer buffer,
            int index,
            int length)
        {
            streamState.onMessage(msgTypeId, buffer, index, length);
        }

        private void streamBeforeBegin(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            if (msgTypeId == BeginFW.TYPE_ID)
            {
                processBegin(buffer, index, length);
            }
            else
            {
                processUnexpected(buffer, index, length);
            }
        }

        private void streamBeforeHeadersWritten(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case EndFW.TYPE_ID:
                endDeferred = true;
                break;
            default:
                slab.release(slotIndex);
                processUnexpected(buffer, index, length);
                break;
            }
        }

        private void streamAfterBeginOrData(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case DataFW.TYPE_ID:
                processData(buffer, index, length);
                break;
            case EndFW.TYPE_ID:
                processEnd(buffer, index, length);
                break;
            default:
                processUnexpected(buffer, index, length);
                break;
            }
        }

        private void streamAfterEnd(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            processUnexpected(buffer, index, length);
        }

        private void streamAfterReplyOrReset(
            int msgTypeId,
            MutableDirectBuffer buffer,
            int index,
            int length)
        {
            if (msgTypeId == DataFW.TYPE_ID)
            {
                dataRO.wrap(buffer, index, index + length);
                final long streamId = dataRO.streamId();

                source.doWindow(streamId, length);
            }
            else if (msgTypeId == EndFW.TYPE_ID)
            {
                endRO.wrap(buffer, index, index + length);
                final long streamId = endRO.streamId();

                source.removeStream(streamId);

                this.streamState = this::streamAfterEnd;
            }
        }

        private void processBegin(
            DirectBuffer buffer,
            int index,
            int length)
        {
            beginRO.wrap(buffer, index, index + length);
            this.sourceId = beginRO.streamId();
            this.sourceRef = beginRO.referenceId();
            this.correlationId = beginRO.correlationId();
            final OctetsFW extension = beginRO.extension();

            // TODO: avoid object creation
            Map<String, String> headers = EMPTY_HEADERS;
            if (extension.sizeof() > 0)
            {
                final HttpBeginExFW beginEx = extension.get(beginExRO::wrap);
                Map<String, String> headers0 = new LinkedHashMap<>();
                beginEx.headers().forEach(h -> headers0.put(h.name().asString(), h.value().asString()));
                headers = headers0;
            }
            final Optional<Route> optional = resolveTarget(sourceRef, headers);

            if (optional.isPresent())
            {
                targetId = supplyTargetId.getAsLong();
                final long targetCorrelationId = targetId;
                final Correlation correlation = new Correlation(correlationId, source.routableName(), INPUT_ESTABLISHED);

                correlateNew.accept(targetCorrelationId, correlation);

                final Route route = optional.get();
                target = route.target();
                final long targetRef = route.targetRef();
                target.doBegin(targetId, targetRef, targetCorrelationId);
                target.setThrottle(targetId, this::handleThrottle);

                String[] pseudoHeaders = new String[4];

                StringBuilder headersChars = new StringBuilder();
                headers.forEach((name, value) ->
                {
                    switch(name.toLowerCase())
                    {
                    case ":method":
                        pseudoHeaders[METHOD] = value;
                        break;
                    case ":scheme":
                        pseudoHeaders[SCHEME] = value;
                        break;
                    case ":authority":
                        pseudoHeaders[AUTHORITY] = value;
                        break;
                    case ":path":
                        pseudoHeaders[PATH] = value;
                        break;
                    case "host":
                        if (pseudoHeaders[AUTHORITY] == null)
                        {
                            pseudoHeaders[AUTHORITY] = value;
                        }
                        else if (!pseudoHeaders[AUTHORITY].equals(value))
                        {
                            processUnexpected(buffer, index, length);
                        }
                        break;
                    default:
                        headersChars.append(toUpperCase(name.charAt(0))).append(name.substring(1))
                        .append(": ").append(value).append("\r\n");
                    }
                });

                if (pseudoHeaders[METHOD] == null || pseudoHeaders[SCHEME] == null || pseudoHeaders[PATH] == null
                        || pseudoHeaders[AUTHORITY] == null)
                {
                    processUnexpected(buffer, index, length);
                }

                String payloadChars =
                        new StringBuilder().append(pseudoHeaders[METHOD]).append(" ").append(pseudoHeaders[PATH])
                                           .append(" HTTP/1.1").append("\r\n")
                                           .append("Host").append(": ").append(pseudoHeaders[AUTHORITY]).append("\r\n")
                                           .append(headersChars).append("\r\n").toString();
                slotIndex = slab.acquire(sourceId);
                slotPosition = 0;
                MutableDirectBuffer slot = slab.buffer(slotIndex);
                if (payloadChars.length() > slot.capacity())
                {
                    // TODO: diagnostics (reset reason?)
                    source.doReset(sourceId);
                    target.removeThrottle(targetId);
                    source.removeStream(sourceId);
                }
                else
                {
                    byte[] bytes = payloadChars.getBytes(US_ASCII);
                    slot.putBytes(0, bytes);
                    slotPosition = bytes.length;
                    slotOffset = 0;
                    this.streamState = this::streamBeforeHeadersWritten;
                    this.throttleState = this::throttleBeforeHeadersWritten;
                }
            }
            else
            {
                processUnexpected(buffer, index, length);
            }
        }

        private void processData(
            DirectBuffer buffer,
            int index,
            int length)
        {
            dataRO.wrap(buffer, index, index + length);

            window -= dataRO.length();
            if (window < 0)
            {
                processUnexpected(buffer, index, length);
            }
            else
            {
                final OctetsFW payload = dataRO.payload();
                target.doData(targetId, payload);
            }
        }

        private void processEnd(
            DirectBuffer buffer,
            int index,
            int length)
        {
            endRO.wrap(buffer, index, index + length);
            doEnd();
        }

        private void doEnd()
        {
            target.removeThrottle(targetId);
            source.removeStream(sourceId);
            this.streamState = this::streamAfterEnd;
        }

        private void processUnexpected(
            DirectBuffer buffer,
            int index,
            int length)
        {
            frameRO.wrap(buffer, index, index + length);

            final long streamId = frameRO.streamId();

            source.doReset(streamId);

            this.streamState = this::streamAfterReplyOrReset;
        }

        private void handleThrottle(
            int msgTypeId,
            MutableDirectBuffer buffer,
            int index,
            int length)
        {
            throttleState.onMessage(msgTypeId, buffer, index, length);
        }

        private void throttleBeforeBegin(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case ResetFW.TYPE_ID:
                processReset(buffer, index, length);
                break;
            default:
                // ignore
                break;
            }
        }

        private void throttleBeforeHeadersWritten(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case WindowFW.TYPE_ID:
                processWindowToWriteRequestHeaders(buffer, index, length);
                break;
            case ResetFW.TYPE_ID:
                processReset(buffer, index, length);
                break;
            default:
                // ignore
                break;
            }
        }

        private void throttleNextWindow(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case WindowFW.TYPE_ID:
                processWindow(buffer, index, length);
                break;
            case ResetFW.TYPE_ID:
                processReset(buffer, index, length);
                break;
            default:
                // ignore
                break;
            }
        }

        private void processWindowToWriteRequestHeaders(
            DirectBuffer buffer,
            int index,
            int length)
        {
            windowRO.wrap(buffer, index, index + length);
            int update = windowRO.update();
            int writableBytes = Math.min(slotPosition - slotOffset, update);
            MutableDirectBuffer slot = slab.buffer(slotIndex);
            target.doData(targetId, slot, slotOffset, writableBytes);
            slotOffset += writableBytes;
            int bytesDeferred = slotPosition - slotOffset;
            if (bytesDeferred == 0)
            {
                slab.release(slotIndex);
                slotIndex = NO_SLOT;
                if (endDeferred)
                {
                    doEnd();
                }
                else
                {
                    streamState = this::streamAfterBeginOrData;
                    throttleState = this::throttleNextWindow;
                    update -= writableBytes;
                    if (update > 0)
                    {
                        doWindow(update);
                    }
                }
            }
        }

        private void processWindow(
            DirectBuffer buffer,
            int index,
            int length)
        {
            windowRO.wrap(buffer, index, index + length);

            final int update = windowRO.update();
            doWindow(update);
        }

        private void doWindow(int update)
        {
            window += update;
            source.doWindow(sourceId, update);
        }

        private void processReset(
            DirectBuffer buffer,
            int index,
            int length)
        {
            resetRO.wrap(buffer, index, index + length);
            slab.release(slotIndex);
            source.doReset(sourceId);
        }

        private Optional<Route> resolveTarget(
            long sourceRef,
            Map<String, String> headers)
        {
            final List<Route> routes = supplyRoutes.apply(sourceRef);
            final Predicate<Route> predicate = headersMatch(headers);

            return routes.stream().filter(predicate).findFirst();
        }
    }
}
