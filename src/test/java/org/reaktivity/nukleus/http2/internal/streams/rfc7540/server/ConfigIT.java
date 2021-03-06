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
import static org.reaktivity.nukleus.http2.internal.Http2ConfigurationTest.HTTP2_ACCESS_CONTROL_ALLOW_ORIGIN_NAME;
import static org.reaktivity.nukleus.http2.internal.Http2ConfigurationTest.HTTP2_SERVER_HEADER_NAME;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.test.ReaktorRule;
import org.reaktivity.reaktor.test.annotation.Configuration;
import org.reaktivity.reaktor.test.annotation.Configure;

public class ConfigIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("net", "org/reaktivity/specification/nukleus/http2/streams/network/rfc7540/config")
        .addScriptRoot("app", "org/reaktivity/specification/nukleus/http2/streams/application/rfc7540/config");

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
    @Configure(name = HTTP2_ACCESS_CONTROL_ALLOW_ORIGIN_NAME, value = "true")
    @Configuration("server.json")
    @Specification({
        "${net}/access.control.allow.origin/client",
        "${app}/access.control.allow.origin/server" })
    public void accessControlAllowOrigin() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configure(name = HTTP2_SERVER_HEADER_NAME, value = "reaktivity")
    @Configuration("server.json")
    @Specification({
        "${net}/server.header/client",
        "${app}/server.header/server" })
    public void server() throws Exception
    {
        k3po.finish();
    }
}
