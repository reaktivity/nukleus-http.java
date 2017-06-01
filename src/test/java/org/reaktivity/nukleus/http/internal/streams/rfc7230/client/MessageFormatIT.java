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

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.test.NukleusRule;

public class MessageFormatIT
{
    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("route", "org/reaktivity/specification/nukleus/http/control/route")
            .addScriptRoot("server", "org/reaktivity/specification/http/rfc7230/message.format")
            .addScriptRoot("client", "org/reaktivity/specification/nukleus/http/streams/rfc7230/message.format");

    private final TestRule timeout = new DisableOnDebug(new Timeout(10, SECONDS));

    private final NukleusRule nukleus = new NukleusRule("http")
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(1024);

    @Rule
    public final TestRule chain = outerRule(nukleus).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${client}/request.with.headers/client",
        "${server}/request.with.headers/server" })
    public void requestWithHeaders() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${client}/request.with.content.length/client",
        "${server}/request.with.content.length/server" })
    public void requestWithContentLength() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${client}/response.with.headers/client",
        "${server}/response.with.headers/server" })
    public void responseWithHeaders() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${client}/response.with.content.length/client",
        "${server}/response.with.content.length/server" })
    public void responseWithContentLength() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${client}/invalid.request.whitespace.after.start.line/client",
        "${server}/invalid.request.whitespace.after.start.line/server" })
    public void invalidRequestWhitespaceAfterStartLine() throws Exception
    {
        // As per RFC, alternatively could process everything before whitespace,
        // but the better choice is to reject
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${client}/invalid.request.missing.target/client",
        "${server}/invalid.request.missing.target/server" })
    public void invalidRequestMissingTarget() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${client}/invalid.request.not.http/client",
        "${server}/invalid.request.not.http/server" })
    public void invalidRequestNotHttp() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${client}/request.with.unimplemented.method/client",
        "${server}/request.with.unimplemented.method/server" })
    public void requestWithUnimplementedMethod() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${client}/request.with.extra.CRLF.after.request.line/client",
        "${server}/request.with.extra.CRLF.after.request.line/server" })
    public void robustServerShouldAllowExtraCRLFAfterRequestLine() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${client}/request.with.start.line.too.long/client",
        "${server}/request.with.start.line.too.long/server" })
    public void requestWithStartLineTooLong() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${client}/invalid.request.space.before.colon.in.header/client",
        "${server}/invalid.request.space.before.colon.in.header/server" })
    public void invalidRequestSpaceBeforeColonInHeader() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${client}/request.with.obsolete.line.folding/client",
        "${server}/request.with.obsolete.line.folding/server" })
    public void requestWithObsoleteLineFolding() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${client}/request.with.header.value.too.long/client",
        "${server}/request.with.header.value.too.long/server" })
    public void requestWithHeaderValueTooLong() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${client}/request.with.unknown.transfer.encoding/client",
        "${server}/request.with.unknown.transfer.encoding/server" })
    public void requestWithUnknownTransferEncoding() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${client}/post.request.with.no.content/client",
        "${server}/post.request.with.no.content/server" })
    public void postRequestWithNoContent() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${client}/head.request.and.response/client",
        "${server}/head.request.and.response/server" })
    public void headRequestAndResponse() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${client}/head.request.and.response.with.content.length/client",
        "${server}/head.request.and.response.with.content.length/server" })
    public void headRequestAndResponseWithContentLength() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${client}/invalid.request.multiple.content.lengths/client",
        "${server}/invalid.request.multiple.content.lengths/server" })
    public void invalidRequestMultipleContentLengths() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${client}/gateway.must.reject.request.with.multiple.different.content.length/client",
        "${gateway}/gateway.must.reject.request.with.multiple.different.content.length/gateway",
        "${server}/gateway.must.reject.request.with.multiple.different.content.length/server" })
    @Ignore("proxy tests not tests implemented")
    public void gatewayMustRejectResponseWithMultipleDifferentContentLength() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${client}/on.response.proxy.must.remove.space.in.header.with.space.between.header.name.and.colon/client",
        "${server}/on.response.proxy.must.remove.space.in.header.with.space.between.header.name.and.colon/server",
        "${proxy}/on.response.proxy.must.remove.space.in.header.with.space.between.header.name.and.colon/proxy" })
    @Ignore("proxy tests not tests implemented")
    public void onResponseProxyMustRemoveSpaceInHeaderWithSpaceBetweenHeaderNameAndColon() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${client}/proxy.or.gateway.must.reject.obs.in.header.value/client",
        "${server}/proxy.or.gateway.must.reject.obs.in.header.value/server" })
    @Ignore("proxy tests not tests implemented")
    public void proxyOrGatewayMustRejectOBSInHeaderValue() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/output/new/controller",
        "${client}/proxy.should.preserve.unrecongnized.headers/client",
        "${server}/proxy.should.preserve.unrecongnized.headers/server",
        "${proxy}/proxy.should.preserve.unrecongnized.headers/proxy" })
    @Ignore("proxy tests not tests implemented")
    public void proxyShouldPreserveUnrecognizedHeaders() throws Exception
    {
        k3po.finish();
    }

}
