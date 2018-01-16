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
import static org.junit.Assert.assertEquals;
import static org.junit.rules.RuleChain.outerRule;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.nukleus.http.internal.HttpConfiguration;
import org.reaktivity.nukleus.http.internal.HttpController;
import org.reaktivity.nukleus.http.internal.test.HttpCountersRule;
import org.reaktivity.reaktor.test.ReaktorRule;

public class ConnectionManagementPoolSize1IT
{
    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("route", "org/reaktivity/specification/nukleus/http/control/route")
            .addScriptRoot("client", "org/reaktivity/specification/nukleus/http/streams/rfc7230/connection.management")
            .addScriptRoot("server", "org/reaktivity/specification/http/rfc7230/connection.management");

    private final TestRule timeout = new DisableOnDebug(new Timeout(10, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .nukleus("http"::equals)
        .controller(HttpController.class::isAssignableFrom)
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(1024)
        .configure(HttpConfiguration.MAXIMUM_CONNECTIONS_PROPERTY_NAME, "1")
        .clean();

    private final HttpCountersRule counters = new HttpCountersRule(reaktor);

    @Rule
    public final TestRule chain = outerRule(reaktor).around(counters).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/concurrent.requests/client",
        "${server}/multiple.requests.same.connection/server" })
    // With connection pool size limited to one the second concurrent request
    // must wait to use the same single connection
    public void concurrentRequestsSameConnection() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("WRITE_RESPONSE_ONE");
        k3po.notifyBarrier("WRITE_RESPONSE_TWO");
        k3po.notifyBarrier("WRITE_RESPONSE_THREE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/concurrent.upgrade.requests.and.responses.with.data/client",
        "${server}/concurrent.upgrade.requests.and.responses.with.data/server" })
    public void connectionsLimitShouldNotApplyToUpgradedConnections() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("REQUEST_ONE_RECEIVED");
        k3po.awaitBarrier("REQUEST_TWO_RECEIVED");
        k3po.notifyBarrier("WRITE_DATA_REQUEST_ONE");
        k3po.notifyBarrier("WRITE_DATA_REQUEST_TWO");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/request.and.502.response/client",
        "${server}/request.incomplete.response.headers.and.abort/server" })
    public void shouldGive502ResponseAndFreeConnectionWhenConnectReplyStreamIsAbortedBeforeResponseHeadersComplete()
            throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/request.and.502.response/client",
        "${server}/request.incomplete.response.headers.and.end/server" })
    public void shouldGive502ResponseAndFreeConnectionWhenConnectReplyStreamEndsBeforeResponseHeadersComplete() throws Exception
    {
        k3po.finish();
    }

    @Ignore("BEGIN vs RESET read order not yet guaranteed to match write order")
    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/request.and.502.response/client",
        "${server}/request.incomplete.response.headers.and.reset/server" })
    public void shouldGive502ResponseAndFreeConnectionWhenConnectStreamIsResetBeforeResponseHeadersComplete() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/request.and.502.response/client",
        "${server}/request.no.response.and.end/server" })
    public void shouldGive502ResponseAndFreeConnectionWhenConnectReplyStreamEndsBeforeResponseReceived() throws Exception
    {
        k3po.finish();
    }

    @Ignore("BEGIN vs RESET read order not yet guaranteed to match write order")
    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/request.and.502.response/client",
        "${server}/request.no.response.and.reset/server" })
    public void shouldGive502ResponseAndFreeConnectionWhenConnectStreamIsResetBeforeResponseReceived() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/request.and.abort/client",
        "${server}/request.and.abort/server"})
    public void shouldAbortTransportAndFreeConnectionWhenRequestIsAborted() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/pending.request.second.request.and.abort/client",
        "${server}/pending.request.second.request.and.abort/server"})
    public void shouldLeaveTransportUntouchedWhenEnqueuedRequestIsAborted() throws Exception
    {
        assertEquals(0, counters.enqueues());
        assertEquals(0, counters.dequeues());
        k3po.finish();
        assertEquals(1, counters.enqueues());
        assertEquals(1, counters.dequeues());
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/request.receive.reset/client",
        "${server}/partial.request.receive.reset/server"})
    public void shouldResetRequestAndFreeConnectionWhenLowLevelIsReset() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/request.and.response.twice/client",
        "${server}/request.response.and.end/server"})
    public void shouldEndOutputAndFreeConnectionWhenEndReceivedAfterCompleteResponse() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/request.and.response.twice.awaiting.barrier/client",
        "${server}/request.response.and.reset/server"})
    public void shouldEndOutputAndFreeConnectionWhenResetReceivedAfterCompleteResponse() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("CONNECTION_RESET");
        k3po.notifyBarrier("ISSUE_SECOND_REQUEST");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/request.and.response.with.incomplete.data.and.abort/client",
        "${server}/request.response.headers.incomplete.data.and.end/server"})
    public void shouldSendAbortAndFreeConnectionWhenConnectReplyStreamEndsBeforeResponseDataComplete() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/request.and.response.with.incomplete.data.and.abort/client",
        "${server}/request.response.headers.incomplete.data.and.abort/server"})
    public void shouldSendAbortAndFreeConnectionWhenConnectReplyStreamIsAbortedBeforeResponseDataComplete() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/request.and.response.with.incomplete.data/client",
        "${server}/request.response.headers.incomplete.data.and.reset/server" })
    public void shouldFreeConnectionWhenConnectStreamIsResetBeforeResponseDataIsComplete() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/response.with.content.length.is.reset/client",
        "${server}/response.with.content.length.is.reset/server" })
    public void shouldResetRequestAndFreeConnectionWhenRequestWithContentLengthIsReset() throws Exception
    {
        k3po.finish();
    }

}
