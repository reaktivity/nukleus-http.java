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
package org.reaktivity.nukleus.http.internal.streams.rfc7230;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.test.NukleusRule;

public class FlowControlIT
{
    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("route", "org/reaktivity/specification/nukleus/http/control/route")
            .addScriptRoot("streams", "org/reaktivity/specification/nukleus/http/streams/rfc7230/flow.control");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    private final NukleusRule nukleus = new NukleusRule("http")
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(1024)
        .streams("http", "source")
        .streams("source", "http#source")
        .streams("target", "http#source")
        .streams("http", "target")
        .streams("source", "http#target");

    @Rule
    public final TestRule chain = outerRule(nukleus).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/input/new/controller",
        "${streams}/request.fragmented/server/source",
        "${streams}/request.fragmented/server/target" })
    public void shouldAcceptFragmentedRequest() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_INPUT");
        k3po.notifyBarrier("ROUTED_OUTPUT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/input/new/controller",
        "${streams}/request.fragmented.with.content.length/server/source",
        "${streams}/request.fragmented.with.content.length/server/target" })
    public void shouldAcceptFragmentedRequestWithContentLength() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_INPUT");
        k3po.notifyBarrier("ROUTED_OUTPUT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/input/new/controller",
        "${streams}/request.with.content.flow.controlled/server/source",
        "${streams}/request.with.content.flow.controlled/server/target" })
    public void shouldSplitRequestDataToRespectTargetWindow() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_INPUT");
        k3po.notifyBarrier("ROUTED_OUTPUT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/input/new/controller",
        "${streams}/request.with.fragmented.content.flow.controlled/server/source",
        "${streams}/request.with.fragmented.content.flow.controlled/server/target" })
    public void shouldSlabDataWhenTargetWindowStillNegative() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_INPUT");
        k3po.notifyBarrier("ROUTED_OUTPUT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/input/new/controller",
        "${streams}/request.with.content.length.and.end.late.target.window/server/source",
        "${streams}/request.with.content.length.and.end.late.target.window/server/target" })
    public void shouldNotProcessSourceEndBeforeGettingWindowAndWritingData() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_INPUT");
        k3po.notifyBarrier("ROUTED_OUTPUT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/input/new/controller",
        "${streams}/response.flow.controlled/server/source",
        "${streams}/response.flow.controlled/server/target" })
    public void shouldFlowControlResponse() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_INPUT");
        k3po.notifyBarrier("ROUTED_OUTPUT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/input/new/controller",
        "${streams}/response.with.content.flow.controlled/server/source",
        "${streams}/response.with.content.flow.controlled/server/target" })
    public void shouldFlowControlResponseWithContent() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_INPUT");
        k3po.notifyBarrier("ROUTED_OUTPUT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${streams}/response.fragmented/client/source",
        "${streams}/response.fragmented/client/target" })
    public void shouldAcceptFragmentedResponse() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_OUTPUT");
        k3po.notifyBarrier("ROUTED_INPUT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${streams}/response.fragmented.with.content.length/client/source",
        "${streams}/response.fragmented.with.content.length/client/target" })
    public void shouldAcceptFragmentedResponseWithContentLength() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_OUTPUT");
        k3po.notifyBarrier("ROUTED_INPUT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${streams}/response.with.content.flow.controlled/client/source",
        "${streams}/response.with.content.flow.controlled/client/target" })
    public void shouldSplitResponseDataToRespectTargetWindow() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_OUTPUT");
        k3po.notifyBarrier("ROUTED_INPUT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${streams}/response.with.fragmented.content.flow.controlled/client/source",
        "${streams}/response.with.fragmented.content.flow.controlled/client/target" })
    public void shouldSlabResponseDataWhenTargetWindowStillNegative() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_OUTPUT");
        k3po.notifyBarrier("ROUTED_INPUT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${streams}/response.with.content.length.and.end.late.target.window/client/source",
        "${streams}/response.with.content.length.and.end.late.target.window/client/target" })
    public void shouldWaitForSourceWindowAndWriteDataBeforeProcessingTargetEnd() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_OUTPUT");
        k3po.notifyBarrier("ROUTED_INPUT");
        k3po.finish();
    }

}
