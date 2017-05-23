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
package org.reaktivity.nukleus.http.internal.streams.rfc7230.agrona;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;
import static org.reaktivity.nukleus.http.internal.Context.MAXIMUM_HEADERS_SIZE_PROPERTY_NAME;
import static org.reaktivity.nukleus.http.internal.Context.MEMORY_FOR_DECODE_PROPERTY_NAME;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.ScriptProperty;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.nukleus.http.internal.test.SystemPropertiesRule;
import org.reaktivity.reaktor.test.NukleusRule;

public class FlowControlLimitsIT
{
    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("route", "org/reaktivity/specification/nukleus/http/control/route")
            .addScriptRoot("streams", "org/reaktivity/specification/nukleus/http/streams/rfc7230/flow.control/agrona");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    private final TestRule properties = new SystemPropertiesRule()
        .setProperty(MAXIMUM_HEADERS_SIZE_PROPERTY_NAME, "64")
        .setProperty(MEMORY_FOR_DECODE_PROPERTY_NAME, "64");

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
    public final TestRule chain = outerRule(properties).around(nukleus).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/input/new/controller",
        "${streams}/request.headers.too.long/server/source" })
    public void shouldRejectRequestExceedingMaximumHeadersSize() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_INPUT");
        k3po.notifyBarrier("ROUTED_OUTPUT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/input/new/controller",
        "${streams}/response.headers.too.long/server/source",
        "${streams}/response.headers.too.long/server/target" })
    public void shouldNotWriteResponseExceedingMaximumHeadersSize() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_INPUT");
        k3po.notifyBarrier("ROUTED_OUTPUT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${streams}/response.headers.too.long/client/source",
        "${streams}/response.headers.too.long/client/target"})
    public void shouldRejectResponseExceedingMaximumHeadersSize() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_OUTPUT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${streams}/request.headers.too.long/client/source" })
    public void shouldNotWriteRequestExceedingMaximumHeadersSize() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_OUTPUT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/input/new/controller",
        "${streams}/request.fragmented.with.content.length/server/source",
        "${streams}/request.fragmented.with.content.length/server/target" })
    @ScriptProperty("targetInputInitialWindow [0x40 0x00 0x00 0x00]") // 64 bytes, same as max headers size
    public void shouldAcceptFragmentedRequestWithDataWhenOnlyDataExceedsMaxHttpHeadersSize() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_INPUT");
        k3po.notifyBarrier("ROUTED_OUTPUT");
        k3po.finish();
    }


    @Test
    @Specification({
        "${route}/output/new/controller",
        "${streams}/response.fragmented.with.content.length/client/source",
        "${streams}/response.fragmented.with.content.length/client/target" })
    @ScriptProperty("sourceInputInitialWindow [0x40 0x00 0x00 0x00]") // 64 bytes, same as max headers size
    public void shouldAcceptFragmentedResponseWithDataWhenOnlyDataExceedsMaxHttpHeadersSize() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_OUTPUT");
        k3po.notifyBarrier("ROUTED_INPUT");
        k3po.finish();
    }

}

