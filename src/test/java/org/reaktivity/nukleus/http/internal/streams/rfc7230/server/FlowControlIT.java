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
package org.reaktivity.nukleus.http.internal.streams.rfc7230.server;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.ScriptProperty;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.test.NukleusRule;

public class FlowControlIT
{
    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("route", "org/reaktivity/specification/nukleus/http/control/route")
            .addScriptRoot("client", "org/reaktivity/specification/http/rfc7230/")
            .addScriptRoot("server", "org/reaktivity/specification/nukleus/http/streams/rfc7230/");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    private final NukleusRule nukleus = new NukleusRule("http")
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(1024);

    @Rule
    public final TestRule chain = outerRule(nukleus).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/flow.control/multiple.requests.pipelined.fragmented/client",
        "${server}/connection.management/multiple.requests.serialized/server" })
    @Ignore("TODO: support pipelined requests, at a minimum by serializing them")
    public void shouldAcceptMultipleRequestsInSameDataFrameFragmented() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connection.management/multiple.requests.pipelined/client",
        "${server}/connection.management/multiple.requests.serialized/server" })
    @Ignore("TODO: support pipelined requests, at a minimum by serializing them")
    @ScriptProperty("clientInitialWindow \"89\"")
    public void shouldFlowControlMultipleResponses() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/flow.control/request.fragmented/client",
        "${server}/message.format/request.with.headers/server" })
    public void shouldAcceptFragmentedRequest() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/message.format/request.with.content.length/client",
        "${server}/message.format/request.with.content.length/server" })
    @ScriptProperty("serverInitialWindow \"3\"")
    public void shouldSplitRequestDataToRespectTargetWindow() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/flow.control/request.fragmented.with.content.length/client",
        "${server}/message.format/request.with.content.length/server" })
    public void shouldAcceptFragmentedRequestWithContentLength() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/flow.control/request.fragmented.with.content.length/client",
        "${server}/message.format/request.with.content.length/server"})
    @ScriptProperty("serverInitialWindow \"3\"")
    public void shouldFlowControlFragmentedRequestWithContent() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/flow.control/request.with.content.length.and.transport.close/client",
        "${server}/message.format/request.with.content.length/server" })
    public void shouldDeferEndProcessingUntilRequestProcessed() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/connection.management/upgrade.request.and.response.with.data/client",
        "${server}/connection.management/upgrade.request.and.response.with.data/server"})
    @ScriptProperty({"clientInitialWindow \"11\"",
                     "serverInitialWindow \"9\""})
    public void shouldFlowControlDataAfterUpgrade() throws Exception
    {
        k3po.finish();
    }

}
