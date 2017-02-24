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
package org.reaktivity.nukleus.http.internal.bench;

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.file.FileVisitOption.FOLLOW_LINKS;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.reaktivity.nukleus.Configuration.DIRECTORY_PROPERTY_NAME;
import static org.reaktivity.nukleus.Configuration.STREAMS_BUFFER_CAPACITY_PROPERTY_NAME;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.Random;

import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.MessageHandler;
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
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.http.internal.HttpController;
import org.reaktivity.nukleus.http.internal.HttpStreams;
import org.reaktivity.nukleus.http.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.http.internal.types.stream.DataFW;
import org.reaktivity.nukleus.http.internal.types.stream.WindowFW;
import org.reaktivity.reaktor.internal.Reaktor;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@Fork(3)
@Warmup(iterations = 5, time = 1, timeUnit = SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = SECONDS)
@OutputTimeUnit(SECONDS)
public class HttpServerBM
{
    private final Configuration configuration;
    private final Reaktor reaktor;

    {
        Properties properties = new Properties();
        properties.setProperty(DIRECTORY_PROPERTY_NAME, "target/nukleus-benchmarks");
        properties.setProperty(STREAMS_BUFFER_CAPACITY_PROPERTY_NAME, Long.toString(1024L * 1024L * 16L));

        configuration = new Configuration(properties);

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

        reaktor = Reaktor.launch(configuration, n -> "http".equals(n), HttpController.class::isAssignableFrom);
    }

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final WindowFW.Builder windowRW = new WindowFW.Builder();

    private HttpStreams sourceInputStreams;
    private HttpStreams sourceOutputEstStreams;

    private MutableDirectBuffer throttleBuffer;

    private long sourceInputRef;
    private long targetInputRef;

    private long sourceInputId;
    private DataFW data;

    private MessageHandler sourceOutputEstHandler;

    @Setup(Level.Trial)
    public void reinit() throws Exception
    {
        final Random random = new Random();
        final HttpController controller = reaktor.controller(HttpController.class);

        this.targetInputRef = random.nextLong();
        this.sourceInputRef = controller.routeInputNew("source", 0L, "target", targetInputRef, emptyMap()).get();

        this.sourceInputStreams = controller.streams("source");
        this.sourceOutputEstStreams = controller.streams("http", "target");

        this.sourceInputId = random.nextLong();
        this.sourceOutputEstHandler = this::processBegin;

        final AtomicBuffer writeBuffer = new UnsafeBuffer(new byte[256]);

        BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .streamId(sourceInputId)
                .referenceId(sourceInputRef)
                .correlationId(random.nextLong())
                .extension(e -> e.reset())
                .build();

        this.sourceInputStreams.writeStreams(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());

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
    }

    @TearDown(Level.Trial)
    public void reset() throws Exception
    {
        HttpController controller = reaktor.controller(HttpController.class);

        controller.unrouteInputNew("source", sourceInputRef, "target", targetInputRef, null).get();

        this.sourceInputStreams.close();
        this.sourceInputStreams = null;

        this.sourceOutputEstStreams.close();
        this.sourceOutputEstStreams = null;
    }

    @Benchmark
    @Group("throughput")
    @GroupThreads(1)
    public void writer(Control control) throws Exception
    {
        while (!control.stopMeasurement &&
               !sourceInputStreams.writeStreams(data.typeId(), data.buffer(), 0, data.limit()))
        {
            Thread.yield();
        }

        while (!control.stopMeasurement &&
                sourceInputStreams.readThrottle((t, b, o, l) -> {}) == 0)
        {
            Thread.yield();
        }
    }

    @Benchmark
    @Group("throughput")
    @GroupThreads(1)
    public void reader(Control control) throws Exception
    {
        while (!control.stopMeasurement &&
               sourceOutputEstStreams.readStreams(this::handleSourceOutputEst) == 0)
        {
            Thread.yield();
        }
    }

    private void handleSourceOutputEst(
        int msgTypeId,
        MutableDirectBuffer buffer,
        int index,
        int length)
    {
        sourceOutputEstHandler.onMessage(msgTypeId, buffer, index, length);
    }

    private void processBegin(
        int msgTypeId,
        MutableDirectBuffer buffer,
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
        MutableDirectBuffer buffer,
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
        final int update)
    {
        final WindowFW window = windowRW.wrap(throttleBuffer, 0, throttleBuffer.capacity())
                .streamId(streamId)
                .update(update)
                .build();

        sourceOutputEstStreams.writeThrottle(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    public static void main(String[] args) throws RunnerException
    {
        Options opt = new OptionsBuilder()
                .include(HttpServerBM.class.getSimpleName())
                .forks(0)
                .build();

        new Runner(opt).run();
    }
}
