/**
 * Copyright 2016-2021 The Reaktivity Project
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
package org.reaktivity.nukleus.http2.internal.streams.rfc7540.server;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;
import static org.reaktivity.nukleus.http2.internal.Http2Configuration.HTTP2_SERVER_CONCURRENT_STREAMS;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.test.ReaktorRule;
import org.reaktivity.reaktor.test.annotation.Configuration;

public class MessageFormatIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("net", "org/reaktivity/specification/nukleus/http2/streams/network/rfc7540/message.format")
        .addScriptRoot("app", "org/reaktivity/specification/nukleus/http2/streams/application/rfc7540/message.format");

    private final TestRule timeout = new DisableOnDebug(new Timeout(10, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(8192)
        .configure(HTTP2_SERVER_CONCURRENT_STREAMS, 100)
        .configurationRoot("org/reaktivity/specification/nukleus/http2/config")
        .external("app#0")
        .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    @Test
    @Configuration("server.json")
    @Specification({
        "${net}/continuation.frames/client",
        "${app}/continuation.frames/server" })
    public void continuationFrames() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("server.json")
    @Specification({
        "${net}/dynamic.table.requests/client",
        "${app}/dynamic.table.requests/server" })
    public void dynamicTableRequests() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("server.json")
    @Specification({
        "${net}/max.frame.size/client",
        "${app}/max.frame.size/server" })
    public void maxFrameSize() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("server.json")
    @Specification({
        "${net}/max.frame.size.error/client",
        "${app}/max.frame.size.error/server" })
    public void maxFrameSizeError() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("server.json")
    @Specification({
        "${net}/ping.frame.size.error/client" })
    public void pingFrameSizeError() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("server.json")
    @Specification({
        "${net}/connection.window.frame.size.error/client" })
    public void connectionWindowFrameSizeError() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("server.json")
    @Specification({
        "${net}/window.frame.size.error/client",
        "${app}/window.frame.size.error/server" })
    public void windowFrameSizeError() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("server.json")
    @Specification({
        "${net}/rst.stream.frame.size.error/client",
        "${app}/rst.stream.frame.size.error/server" })
    public void rstStreamFrameSizeError() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("server.json")
    @Specification({
        "${net}/priority.frame.size.error/client",
        "${app}/priority.frame.size.error/server" })
    public void priorityFrameSizeError() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("server.json")
    @Specification({
        "${net}/connection.headers/client",
        "${app}/connection.headers/server" })
    public void connectionHeaders() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("server.json")
    @Specification({
        "${net}/stream.id.order/client",
        "${app}/stream.id.order/server" })
    public void streamIdOrder() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("server.json")
    @Specification({
        "${net}/invalid.hpack.index/client" })
    public void invalidHpackIndex() throws Exception
    {
        k3po.finish();
    }
}
