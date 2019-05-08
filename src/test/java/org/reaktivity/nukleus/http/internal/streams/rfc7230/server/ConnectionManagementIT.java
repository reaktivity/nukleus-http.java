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
package org.reaktivity.nukleus.http.internal.streams.rfc7230.server;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;
import static org.reaktivity.reaktor.test.ReaktorRule.EXTERNAL_AFFINITY_MASK;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.test.ReaktorRule;

public class ConnectionManagementIT
{
    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("route", "org/reaktivity/specification/nukleus/http/control/route")
            .addScriptRoot("client", "org/reaktivity/specification/http/rfc7230/connection.management")
            .addScriptRoot("server", "org/reaktivity/specification/nukleus/http/streams/rfc7230/connection.management");

    private final TestRule timeout = new DisableOnDebug(new Timeout(10, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .nukleus("http"::equals)
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(8192)
        .affinityMask("target#0", EXTERNAL_AFFINITY_MASK)
        .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/request.with.connection.close/client",
        "${server}/request.with.connection.close/server" })
    @Ignore("TODO: SourceOutputEstablishedStream should propagate high-level END after connection:close")
    public void clientAndServerMustCloseConnectionAfterRequestWithConnectionClose() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}response.with.connection.close/client",
        "${server}response.with.connection.close/server" })
    @Ignore("TODO: SourceOutputEstablishedStream should propagate high-level END after connection:close")
    public void serverMustCloseConnectionAfterResponseWithConnectionClose() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/multiple.requests.same.connection/client",
        "${server}/multiple.requests.serialized/server" })
    public void shouldSupportMultipleRequestsOnSameConnection() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/multiple.requests.pipelined/client",
        "${server}/concurrent.requests/server" })
    @Ignore("TODO: support pipelined requests, at a minimum by serializing them")
    public void shouldSupporttHttpPipelining() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/multiple.requests.pipelined.with.retry/client",
        "${server}/multiple.requests.pipelined.with.retry/server" })
    @Ignore("TODO: nukleus scripts")
    public void clientWithPipeliningMustNotRetryPipeliningImmediatelyAfterFailure() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/first.pipelined.response.has.connection.close/client",
        "${server}/first.pipelined.response.has.connection.close/server" })
    public void clientMustNotReuseConnectionWhenReceivesConnectionClose() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/upgrade.request.and.response/client",
        "${server}/upgrade.request.and.response/server" })
    public void serverGettingUpgradeRequestMustRespondWithUpgradeHeader() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server.authority/controller",
        "${client}/request.authority.with.no.port/client",
        "${server}/request.authority.with.no.port/server" })
    public void shouldHandleRequestAuthorityWithNoPort() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server.authority/controller",
        "${client}/request.authority.with.port/client",
        "${server}/request.authority.with.no.port/server" })
    public void shouldHandleRequestAuthorityWithExplicitPort() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/send.end.after.upgrade.request.completed/client",
        "${server}/send.end.after.upgrade.request.completed/server" })
    public void shouldSendEndWhenEndReceivedAfterUpgradeRequestCompleted() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/upgrade.request.and.abort/client",
        "${server}/upgrade.request.and.abort/server" })
    public void serverGettingAbortShouldPropagateAbortOnAllDirections() throws Exception
    {
        k3po.finish();
    }


    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/request.and.upgrade.required.response/client",
        "${server}/request.and.upgrade.required.response/server" })
    public void serverThatSendsUpgradeRequiredMustIncludeUpgradeHeader() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
        "${client}/upgrade.request.and.response.with.data/client",
        "${server}/upgrade.request.and.response.with.data/server" })
    public void serverThatIsUpgradingMustSendA101ResponseBeforeData() throws Exception
    {
        k3po.finish();
    }

    // Proxy tests only have "cooked" versions

    /**
     * See <a href="https://tools.ietf.org/html/rfc7230#section-6.1">RFC 7230 section 6.1: Connection</a>.
     *
     * In order to avoid confusing downstream recipients, a proxy or gateway MUST remove or replace any received
     * connection options before forwarding the message.
     *
     * @throws Exception when K3PO is not started
     */
    // @Test // "http proxy not yet implemented"
    @Specification({
        "${route}/server/controller",
        "${server}/proxy.must.not.forward.connection.header/client",
        "${server}/proxy.must.not.forward.connection.header/proxy",
        "${server}/proxy.must.not.forward.connection.header/backend" })
    @Ignore("http proxy not yet implemented")
    public void intermediaryMustRemoveConnectionHeaderOnForwardRequest() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/server/controller",
            "${server}/reverse.proxy.connection.established/client",
            "${server}/reverse.proxy.connection.established/proxy",
            "${server}/reverse.proxy.connection.established/backend" })
    @Ignore("http proxy not yet implemented")
    public void reverseProxyConnectionEstablished() throws Exception
    {
        k3po.finish();
    }

    /**
     * See <a href="https://tools.ietf.org/html/rfc7230#section-6.3.1">RFC 7230 section 6.3.1: Retrying Requests</a>.
     *
     * A proxy MUST NOT automatically retry non-idempotent requests.
     *
     * @throws Exception when K3PO is not started
     */
    @Test
    @Specification({
        "${route}/server/controller",
        "${server}/proxy.must.not.retry.non.idempotent.requests/client",
        "${server}/proxy.must.not.retry.non.idempotent.requests/proxy",
        "${server}/proxy.must.not.retry.non.idempotent.requests/backend" })
    @Ignore("http proxy not yet implemented")
    public void proxyMustNotRetryNonIdempotentRequests() throws Exception
    {
        k3po.finish();
    }

}
