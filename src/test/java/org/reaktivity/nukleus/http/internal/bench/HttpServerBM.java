/**
 * Copyright 2016-2019 The Reaktivity Project
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
package org.reaktivity.nukleus.http.internal.bench;

import static java.lang.String.format;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.file.FileVisitOption.FOLLOW_LINKS;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.IoUtil.ensureDirectoryExists;
import static org.reaktivity.reaktor.ReaktorConfiguration.REAKTOR_DIRECTORY;
import static org.reaktivity.reaktor.ReaktorConfiguration.REAKTOR_STREAMS_BUFFER_CAPACITY;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.Random;
import java.util.function.ToIntFunction;

import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Control;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.http.internal.HttpController;
import org.reaktivity.nukleus.http.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.http.internal.types.stream.WindowFW;
import org.reaktivity.reaktor.Reaktor;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@Fork(3)
@Warmup(iterations = 1, time = 10, timeUnit = SECONDS)
@Measurement(iterations = 3, time = 10, timeUnit = SECONDS)
@OutputTimeUnit(SECONDS)
public class HttpServerBM
{
    @State(Scope.Group)
    public static class GroupState
    {
        private final Configuration configuration;
        private final Reaktor reaktor;

        {
            Properties properties = new Properties();
            properties.setProperty(REAKTOR_DIRECTORY.name(), "target/nukleus-benchmarks");
            properties.setProperty(REAKTOR_STREAMS_BUFFER_CAPACITY.name(), Long.toString(1024L * 1024L * 16L));

            configuration = new Configuration(properties);
            ensureDirectoryExists(configuration.directory().toFile(), configuration.directory().toString());

            try
            {
                Files.walk(configuration.directory(), FOLLOW_LINKS)
                     .map(Path::toFile)
                     .forEach(File::delete);
            }
            catch (IOException ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }

            reaktor = Reaktor.builder()
                             .config(configuration)
                             .nukleus("http"::equals)
                             .controller("http"::equals)
                             .errorHandler(ex -> ex.printStackTrace(System.err))
                             .build();
        }

        private final BeginFW beginRO = new BeginFW();
        private final DataFW dataRO = new DataFW();
        private final WindowFW windowRO = new WindowFW();

        private final BeginFW.Builder beginRW = new BeginFW.Builder();
        private final DataFW.Builder dataRW = new DataFW.Builder();
        private final WindowFW.Builder windowRW = new WindowFW.Builder();

        private MutableDirectBuffer throttleBuffer;

        private long sourceRouteId;
        private Writer sourceInput;
        private Reader sourceOutputEst;

        private long sourceInputId;
        private DataFW data;

        private MessageConsumer sourceOutputEstHandler;
        int availableSourceInputWindow = 0;
        int padding;
        public int writeFails;
        public int readFails;

        @Setup(Level.Trial)
        public void reinit() throws Exception
        {
            final Random random = new Random();
            final HttpController controller = reaktor.controller(HttpController.class);

            this.sourceRouteId = controller.routeServer("http#0", "echo", emptyMap()).get();

            this.sourceInputId = random.nextLong();
            this.sourceOutputEstHandler = this::processBegin;

            final AtomicBuffer writeBuffer = new UnsafeBuffer(new byte[256]);

            BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                    .routeId(sourceRouteId)
                    .streamId(sourceInputId)
                    .extension(e -> e.reset())
                    .build();

            this.sourceInput.streams.test(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());

            String payload =
                    "POST / HTTP/1.1\r\n" +
                    "Host: localhost:8080\r\n" +
                    "Content-Length:12\r\n" +
                    "\r\n" +
                    "Hello, world";
            byte[] sendArray = payload.getBytes(StandardCharsets.UTF_8);

            this.data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                              .streamId(sourceInputId)
                              .payload(p -> p.set(sendArray))
                              .extension(e -> e.reset())
                              .build();

            this.throttleBuffer = new UnsafeBuffer(allocateDirect(SIZE_OF_LONG + SIZE_OF_INT));

            boolean writeSucceeded = false;
            for (int i = 0; i < 100 && !writeSucceeded; i++)
            {
                Thread.sleep(100);
                writeSucceeded = write();
            }

            if (writeSucceeded)
            {
                for (int i = 0; i < 100 && sourceOutputEst == null; i++)
                {
                    try
                    {
                        // TODO: sourceOutputEst = controller.supplyTarget("http", Reader::new);
                    }
                    catch (IllegalStateException e)
                    {
                        Thread.sleep(100);
                    }
                }
                int result = read();
                if (result <= 0)
                {
                    throw new RuntimeException("reinit: read() failed");
                }
            }
            else
            {
                throw new RuntimeException("reinit: write() failed");
            }

        }

        @TearDown(Level.Trial)
        public void reset() throws Exception
        {
            HttpController controller = reaktor.controller(HttpController.class);

            controller.unroute(sourceRouteId).get();

            this.sourceInput = null;

            this.sourceOutputEst = null;
        }

        private int read()
        {
            return sourceOutputEst.streams.applyAsInt(this::handleSourceOutputEst);
        }

        private boolean write()
        {
            sourceInput.throttle.applyAsInt(this::sourceInputThrottle);
            boolean result = availableSourceInputWindow >= data.length();
            if (result)
            {
                result = sourceInput.streams.test(data.typeId(), data.buffer(), 0, data.limit());
                if (result)
                {
                    availableSourceInputWindow -= data.length();
                }
                else
                {
                    System.out.println(format("write failed, availableSourceInputWindow = %d", availableSourceInputWindow));
                }
            }
            return result;
        }

        private void sourceInputThrottle(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case WindowFW.TYPE_ID:
                windowRO.wrap(buffer, index, index + length);
                availableSourceInputWindow += windowRO.credit();
                padding = windowRO.padding();
                break;
            case ResetFW.TYPE_ID:
                System.out.println("ERROR: reset detected in sourceInputThrottle");
                break;
            default:
                System.out.println(format("ERROR: unexpected msgTypeId %d detected in sourceInputThrottle",
                        msgTypeId));
                break;
            }
        }

        private void handleSourceOutputEst(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            sourceOutputEstHandler.accept(msgTypeId, buffer, index, length);
        }

        private void processBegin(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            beginRO.wrap(buffer, index, index + length);
            final long streamId = beginRO.streamId();
            doWindow(streamId, 8192);

            this.sourceOutputEstHandler = this::processData;
        }

        private void processData(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            dataRO.wrap(buffer, index, index + length);
            final long streamId = dataRO.streamId();
            final int update = dataRO.length();
            doWindow(streamId, update);
        }

        private void doWindow(
            final long streamId,
            final int credit)
        {
            final WindowFW window = windowRW.wrap(throttleBuffer, 0, throttleBuffer.capacity())
                    .streamId(streamId)
                    .credit(credit)
                    .build();
            sourceOutputEst.throttle.test(window.typeId(), window.buffer(), window.offset(), window.sizeof());
        }

        class Reader
        {
            private final ToIntFunction<MessageConsumer> streams;
            private final MessagePredicate throttle;

            Reader(ToIntFunction<MessageConsumer> streams, MessagePredicate throttle)
            {
                this.throttle = throttle;
                this.streams = streams;
            }
        }

        class Writer
        {
            private final MessagePredicate streams;
            private final ToIntFunction<MessageConsumer> throttle;

            Writer(MessagePredicate streams, ToIntFunction<MessageConsumer> throttle)
            {
                this.streams = streams;
                this.throttle = throttle;
            }
        }
    }

    @Benchmark
    @Group("throughput")
    @GroupThreads(1)
    public int writer(final GroupState state, final Control control) throws Exception
    {
        boolean result;
        while (!(result = state.write()) && !control.stopMeasurement)
        {
            Thread.yield();
        }
        return result ? 1 : 0;
    }

    @Benchmark
    @Group("throughput")
    @GroupThreads(1)
    public int reader(final GroupState state, final Control control) throws Exception
    {
        int result;
        while ((result = state.read()) == 0 && !control.stopMeasurement)
        {
            Thread.yield();
        }
        return result;
    }

    public static void main(String[] args) throws RunnerException
    {
        Options opt = new OptionsBuilder()
                .include(HttpServerBM.class.getSimpleName())
                .forks(0)
                .threads(1)
                .warmupIterations(0)
                .measurementIterations(1)
                .measurementTime(new TimeValue(10, SECONDS))
                .build();

        new Runner(opt).run();
    }
}
