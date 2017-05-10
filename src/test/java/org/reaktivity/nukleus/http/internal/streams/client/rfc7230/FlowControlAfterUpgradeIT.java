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
package org.reaktivity.nukleus.http.internal.streams.client.rfc7230;

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

public class FlowControlAfterUpgradeIT
{
    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("route", "org/reaktivity/specification/nukleus/http/control/route")
            .addScriptRoot("streams", "org/reaktivity/specification/nukleus/http/streams/rfc7230/flow.control");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    private final TestRule properties = new SystemPropertiesRule()
        .setProperty(MAXIMUM_HEADERS_SIZE_PROPERTY_NAME, "128")
        .setProperty(MEMORY_FOR_DECODE_PROPERTY_NAME, "128");

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
        "${streams}/request.with.upgrade.and.data/server/source",
        "${streams}/request.with.upgrade.and.data/server/target" })
    @ScriptProperty("targetInputInitialWindow [0x80 0x00 0x00 0x00]") // 128 bytes, same as max headers size
    public void shouldFlowControlRequestDataAfterUpgrade() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_INPUT");
        k3po.notifyBarrier("ROUTED_OUTPUT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/input/new/controller",
        "${streams}/response.with.upgrade.and.data/server/source",
        "${streams}/response.with.upgrade.and.data/server/target" })
    @ScriptProperty("sourceOutputInitialWindow [0x80 0x00 0x00 0x00]") // 128 bytes, same as max headers size
    public void shouldFlowControlResponseDataAfterUpgrade() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_INPUT");
        k3po.notifyBarrier("ROUTED_OUTPUT");
        k3po.finish();
    }
}

