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
package org.reaktivity.nukleus.http.internal.streams.rfc7230.client;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.ScriptProperty;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.test.ReaktorRule;

public class FlowControlIT
{
    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("route", "org/reaktivity/specification/nukleus/http/control/route")
            .addScriptRoot("server", "org/reaktivity/specification/http/rfc7230")
            .addScriptRoot("client", "org/reaktivity/specification/nukleus/http/streams/rfc7230");

    private final TestRule timeout = new DisableOnDebug(new Timeout(10, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .nukleus("http"::equals)
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(1024);

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/architecture/request.and.response/client",
        "${server}/architecture/request.and.response/server" })
    @ScriptProperty("serverInitialWindow \"3\"")
    public void shouldFlowControlRequest() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/message.format/request.with.content.length/client",
        "${server}/message.format/request.with.content.length/server"})
    @ScriptProperty("serverInitialWindow \"9\"")
    public void shouldFlowControlRequestWithContent() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/connection.management/upgrade.request.and.response.with.data/client",
        "${server}/connection.management/upgrade.request.and.response.with.data/server"})
    @ScriptProperty({"clientInitialWindow \"11\"",
                     "serverInitialWindow \"9\""})
    public void shouldFlowControlDataAfterUpgrade() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/message.format/response.with.headers/client",
        "${server}/flow.control/response.fragmented/server"})
    public void shouldProcessFragmentedResponse() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/message.format/response.with.content.length/client",
        "${server}/flow.control/response.fragmented.with.content.length/server"})
    public void shouldProcessFragmentedResponseWithContentLength() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/message.format/response.with.content.length/client",
        "${server}/message.format/response.with.content.length/server"})
    @ScriptProperty("clientInitialWindow \"9\"")
    public void shouldFlowControlResponseWithContentLength() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/message.format/response.with.content.length/client",
        "${server}/flow.control/response.fragmented.with.content.length/server"})
    @ScriptProperty("clientInitialWindow \"9\"")
    public void shouldFlowControlFragmenteResponseWithContentLength() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/message.format/response.with.content.length//client",
        "${server}/flow.control/response.with.content.length.and.transport.close/server" })
    public void shouldDeferEndProcessingUntilResponseProcessed() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/connection.management/multiple.requests.serialized/client",
        "${server}/connection.management/multiple.requests.same.connection/server" })
    @ScriptProperty("serverInitialWindow 16")
    public void shouldProcessFragmentedRequests() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("WRITE_RESPONSE_ONE");
        k3po.notifyBarrier("WRITE_RESPONSE_TWO");
        k3po.notifyBarrier("WRITE_RESPONSE_THREE");
        k3po.finish();
    }
}
