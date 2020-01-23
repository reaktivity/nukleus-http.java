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
package org.reaktivity.nukleus.http2.internal.streams.rfc7540.server;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;
import static org.reaktivity.nukleus.http2.internal.Http2Configuration.HTTP2_SERVER_CONCURRENT_STREAMS;
import static org.reaktivity.nukleus.http2.internal.Http2ConfigurationTest.HTTP2_MAX_CONCURRENT_STREAMS_CLEANUP_NAME;
import static org.reaktivity.nukleus.http2.internal.Http2ConfigurationTest.HTTP2_STREAMS_CLEANUP_DELAY_NAME;
import static org.reaktivity.reaktor.test.ReaktorRule.EXTERNAL_AFFINITY_MASK;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.test.ReaktorRule;
import org.reaktivity.reaktor.test.annotation.Configure;

public class ConnectionManagementIT
{
    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("route", "org/reaktivity/specification/nukleus/http2/control/route")
            .addScriptRoot("spec", "org/reaktivity/specification/http2/rfc7540/connection.management")
            .addScriptRoot("nukleus", "org/reaktivity/specification/nukleus/http2/streams/rfc7540/connection.management");

    private final TestRule timeout = new DisableOnDebug(new Timeout(10, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
            .directory("target/nukleus-itests")
            .commandBufferCapacity(1024)
            .responseBufferCapacity(1024)
            .counterValuesBufferCapacity(8192)
            .nukleus("http2"::equals)
            .configure(HTTP2_SERVER_CONCURRENT_STREAMS, 100)
            .affinityMask("target#0", EXTERNAL_AFFINITY_MASK)
            .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/connection.established/client" })
    public void connectionEstablished() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/http.get.exchange/client",
            "${nukleus}/http.get.exchange/server" })
    public void httpGetExchange() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server.override/controller",
            "${spec}/http.get.exchange.with.header.override/client",
            "${nukleus}/http.get.exchange.with.header.override/server" })
    public void httpGetExchangeWithHeaderOverride() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/http.unknown.authority/client" })
    public void httpUnknownAuthority() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/http.post.exchange/client",
            "${nukleus}/http.post.exchange/server" })
    public void httpPostExchange() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/http.post.exchange.streaming/client",
            "${nukleus}/http.post.exchange.streaming/server" })
    public void httpPostExchangeWhenStreaming() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/connection.has.two.streams/client",
            "${nukleus}/connection.has.two.streams/server" })
    public void connectionHasTwoStreams() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/http.push.promise/client",
            "${nukleus}/http.push.promise/server" })
    public void pushResources() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/push.promise.on.different.stream/client",
            "${nukleus}/push.promise.on.different.stream/server" })
    public void pushPromiseOnDifferentStream() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/multiple.data.frames/client",
            "${nukleus}/multiple.data.frames/server" })
    public void multipleDataFrames() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/reset.http2.stream/client",
            "${nukleus}/reset.http2.stream/server" })
    public void resetHttp2Stream() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/ignore.rst.stream/client",
            "${nukleus}/ignore.rst.stream/server" })
    public void ignoreRsttStream() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/client.sent.read.abort.on.open.request/client",
            "${nukleus}/client.sent.read.abort.on.open.request/server"
    })
    public void clientSentReadAbortOnOpenRequest() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/client.sent.read.abort.on.closed.request/client",
            "${nukleus}/client.sent.read.abort.on.closed.request/server"
    })
    public void clientSentReadAbortOnClosedRequest() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/client.sent.write.abort.on.open.request/client",
            "${nukleus}/client.sent.write.abort.on.open.request/server"
    })
    public void clientSentWriteAbortOnOpenRequest() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/client.sent.write.abort.on.closed.request/client",
            "${nukleus}/client.sent.write.abort.on.closed.request/server"
    })
    public void clientSentWriteAbortOnClosedRequest() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/client.sent.write.close/client",
            "${nukleus}/client.sent.write.close/server"
    })
    public void clientSentWriteClose() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/server.sent.read.abort.on.open.request/client",
            "${nukleus}/server.sent.read.abort.on.open.request/server"
    })
    public void serverSentReadAbortOnOpenRequest() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/server.sent.read.abort.before.correlated/client",
            "${nukleus}/server.sent.read.abort.before.correlated/server"
    })
    public void serverSentReadAbortBeforeCorrelated() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/rst.stream.last.frame/client",
            "${nukleus}/rst.stream.last.frame/server"
    })
    public void rstStreamLastFrame() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/server.sent.write.abort.on.open.request/client",
            "${nukleus}/server.sent.write.abort.on.open.request/server"
    })
    public void serverSentWriteAbortOnOpenRequest() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/server.sent.write.abort.on.closed.request/client",
            "${nukleus}/server.sent.write.abort.on.closed.request/server"
    })
    public void serverSentWriteAbortOnClosedRequest() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/server/controller",
            "${spec}/server.sent.write.close/client",
            "${nukleus}/server.sent.write.close/server"
    })
    public void serverSentWriteClose() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server.authority/controller",
        "${spec}/http.authority.default.port/client",
        "${nukleus}/http.authority.default.port/server" })
    public void defaultPortToAuthority() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${spec}/http.response.trailer/client",
        "${nukleus}/http.response.trailer/server" })
    public void shouldProxyResponseTrailer() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${spec}/client.sent.end.before.response.received/client",
        "${nukleus}/client.sent.end.before.response.received/server" })
    public void shouldSendResetOnIncompleteResponse() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server.override/controller",
        "${spec}/http.push.promise.header.override/client",
        "${nukleus}/http.push.promise.header.override/server" })
    public void pushResourcesWithOverrideHeader() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configure(name = HTTP2_MAX_CONCURRENT_STREAMS_CLEANUP_NAME, value = "1")
    @Configure(name = HTTP2_STREAMS_CLEANUP_DELAY_NAME, value = "10")
    @Specification({
        "${route}/server/controller",
        "${spec}/client.sent.write.abort.then.read.abort.on.open.request/client",
        "${nukleus}/client.sent.write.abort.then.read.abort.on.open.request/server"
    })
    public void clientSentWriteAbortThenReadAbortOnOpenRequest() throws Exception
    {
        k3po.finish();
    }

}
