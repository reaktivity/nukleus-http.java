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
import org.reaktivity.specification.nukleus.NukleusRule;

public class MessageFormatIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("scripts", "org/reaktivity/specification/http/rfc7230/message.format");

    private final NukleusRule nukleus = new NukleusRule()
            .directory("target/nukleus-itests");

    private final TestRule timeout = new DisableOnDebug(new Timeout(10, SECONDS));

    @Rule
    public final TestRule chain = outerRule(k3po).around(nukleus).around(timeout);

    /**
     * See <a href="https://tools.ietf.org/html/rfc7230#section-3">RFC 7230 section 3: Message Format</a>.
     *
     * <blockquote> All HTTP/1.1 messages consist of a start-line followed by a sequence of octets in a format similar
     * to the Internet Message Format [RFC5322]: zero or more header fields (collectively referred to as the "headers"
     * or the "header section"), an empty line indicating the end of the header section, and an optional message body.
     * </blockquote>
     *
     * @throws Exception when K3PO is not started
     */
    @Test
    @Specification({
            "${scripts}/request.with.headers/client",
            "${scripts}/request.with.headers/server" })
    @ScriptProperty("serverTransport \"nukleus://http/streams/source\"")
    public void requestWithHeaders() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_INPUT");
        k3po.finish();
    }

    /**
     * See <a href="https://tools.ietf.org/html/rfc7230#section-3">RFC 7230 section 3: Message Format</a>.
     *
     * @throws Exception when K3PO is not started
     */
    @Test
    @Specification({
            "${scripts}/request.with.content.length/client",
            "${scripts}/request.with.content.length/server" })
    @ScriptProperty("serverTransport \"nukleus://http/streams/source\"")
    public void requestWithContentLength() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_INPUT");
        k3po.finish();
    }

    /**
     * See <a href="https://tools.ietf.org/html/rfc7230#section-3">RFC 7230 section 3: Message Format</a>.
     *
     * <blockquote> All HTTP/1.1 messages consist of a start-line followed by a sequence of octets in a format similar
     * to the Internet Message Format [RFC5322]: zero or more header fields (collectively referred to as the "headers"
     * or the "header section"), an empty line indicating the end of the header section, and an optional message body.
     * </blockquote>
     *
     * @throws Exception when K3PO is not started
     */
    @Test
    @Specification({
            "${scripts}/response.with.headers/client",
            "${scripts}/response.with.headers/server" })
    @ScriptProperty("serverTransport \"nukleus://http/streams/source\"")
    public void responseWithHeaders() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_INPUT");
        k3po.finish();
    }

    /**
     * See <a href="https://tools.ietf.org/html/rfc7230#section-3">RFC 7230 section 3: Message Format</a>.
     *
     * @throws Exception when K3PO is not started
     */
    @Test
    @Specification({
            "${scripts}/response.with.content.length/client",
            "${scripts}/response.with.content.length/server" })
    @ScriptProperty("serverTransport \"nukleus://http/streams/source\"")
    public void responseWithContentLength() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_INPUT");
        k3po.finish();
    }

    /**
     * See <a href="https://tools.ietf.org/html/rfc7230#section-3">RFC 7230 section 3: Message Format</a>.
     *
     * A sender MUST NOT send whitespace between the start-line and the first header field. A recipient that receives
     * whitespace between the start-line and the first header field MUST either reject the message as invalid or consume
     * each whitespace-preceded line without further processing of it (i.e., ignore the entire line, along with any
     * subsequent lines preceded by whitespace, until a properly formed header field is received or the header section
     * is terminated).
     *
     * @throws Exception when K3PO is not started
     */
    @Test
    @Specification({
            "${scripts}/invalid.request.whitespace.after.start.line/client",
            "${scripts}/invalid.request.whitespace.after.start.line/server" })
    @ScriptProperty("serverTransport \"nukleus://http/streams/source\"")
    public void invalidRequestWhitespaceAfterStartLine() throws Exception
    {
        // As per RFC, alternatively could process everything before whitespace,
        // but the better choice is to reject
        k3po.start();
        k3po.notifyBarrier("ROUTED_INPUT");
        k3po.finish();
    }

    /**
     * See <a href="https://tools.ietf.org/html/rfc7230#section-3">RFC 7230 section 3: Message Format</a>.
     * Request line must contain method, target and http/version
     * @throws Exception when K3PO is not started
     */
    @Test
    @Specification({
            "${scripts}/invalid.request.missing.target/client",
            "${scripts}/invalid.request.missing.target/server" })
    @ScriptProperty("serverTransport \"nukleus://http/streams/source\"")
    public void invalidRequestMissingTarget() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_INPUT");
        k3po.finish();
    }

    /**
     * See <a href="https://tools.ietf.org/html/rfc7230#section-3">RFC 7230 section 3: Message Format</a>.
     *
     * @throws Exception when K3PO is not started
     */
    @Test
    @Specification({
            "${scripts}/invalid.request.not.http/client",
            "${scripts}/invalid.request.not.http/server" })
    @ScriptProperty("serverTransport \"nukleus://http/streams/source\"")
    public void invalidRequestNotHttp() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_INPUT");
        k3po.finish();
    }

    /**
     * See <a href="https://tools.ietf.org/html/rfc7230#section-3">RFC 7230 section 3: Message Format</a>.
     *
     * @throws Exception when K3PO is not started
     */
    @Test
    @Specification({
            "${scripts}/request.with.unimplemented.method/client",
            "${scripts}/request.with.unimplemented.method/server" })
    @ScriptProperty("serverTransport \"nukleus://http/streams/source\"")
    public void requestWithUnimplementedMethod() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_INPUT");
        k3po.finish();
    }

    /**
     * See <a href="https://tools.ietf.org/html/rfc7230#section-3">RFC 7230 section 3: Message Format</a>.
     *
     * In the interest of robustness, a server that is expecting to receive and parse a request-line SHOULD ignore at
     * least one empty line (CRLF) received prior to the request-line.
     *
     * @throws Exception when K3PO is not started
     */
    @Test
    @Specification({
            "${scripts}/request.with.extra.CRLF.after.request.line/client",
            "${scripts}/request.with.extra.CRLF.after.request.line/server" })
    @ScriptProperty("serverTransport \"nukleus://http/streams/source\"")
    public void robustServerShouldAllowExtraCRLFAfterRequestLine() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_INPUT");
        k3po.finish();
    }

    /**
     * See <a href="https://tools.ietf.org/html/rfc7230#section-3">RFC 7230 section 3: Message Format</a>.
     *
     * @throws Exception when K3PO is not started
     */
    @Test
    @Specification({
            "${scripts}/request.with.start.line.too.long/client",
            "${scripts}/request.with.start.line.too.long/server" })
    @ScriptProperty("serverTransport \"nukleus://http/streams/source\"")
    public void requestWithStartLineTooLong() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_INPUT");
        k3po.finish();
    }

    /**
     * See <a href="https://tools.ietf.org/html/rfc7230#section-3">RFC 7230 section 3: Message Format</a>.
     *
     * No whitespace is allowed between the header field-name and colon. In the past, differences in the handling of
     * such whitespace have led to security vulnerabilities in request routing and response handling. A server MUST
     * reject any received request message that contains whitespace between a header field-name and colon with a
     * response code of 400 (Bad Request). A proxy MUST remove any such whitespace from a response message before
     * forwarding the message downstream.
     *
     * @throws Exception when K3PO is not started
     */
    @Test
    @Specification({
            "${scripts}/invalid.request.space.before.colon.in.header/client",
            "${scripts}/invalid.request.space.before.colon.in.header/server" })
    @ScriptProperty("serverTransport \"nukleus://http/streams/source\"")
    public void invalidRequestSpaceBeforeColonInHeader() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_INPUT");
        k3po.finish();
    }

    /**
     * See <a href="https://tools.ietf.org/html/rfc7230#section-3">RFC 7230 section 3: Message Format</a>.
     *
     * A server that receives an obs-fold (new line plus one or more space to extend a header line over multiple
     * lines) in a request message that is not within a message/http container MUST either
     * reject the message by sending a 400 (Bad Request), preferably with a representation explaining that obsolete line
     * folding is unacceptable, or replace each received obs-fold with one or more SP octets prior to interpreting the
     * field value or forwarding the message downstream.
     *
     * @throws Exception when K3PO is not started
     */
    @Test
    @Specification({
            "${scripts}/request.with.obsolete.line.folding/client",
            "${scripts}/request.with.obsolete.line.folding/server" })
    @ScriptProperty("serverTransport \"nukleus://http/streams/source\"")
    public void requestWithObsoleteLineFolding() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_INPUT");
        k3po.finish();
    }

    /**
     * See <a href="https://tools.ietf.org/html/rfc7230#section-3.2.5">RFC 7230 section 3.2.5 Field limits</a>.
     *
     * @throws Exception when K3PO is not started
     */
    @Test
    @Specification({
            "${scripts}/request.with.header.value.too.long/client",
            "${scripts}/request.with.header.value.too.long/server" })
    @ScriptProperty("serverTransport \"nukleus://http/streams/source\"")
    public void requestWithHeaderValueTooLong() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_INPUT");
        k3po.finish();
    }

    /**
     * See <a href="https://tools.ietf.org/html/rfc7230#section-3">RFC 7230 section 3: Message Format</a>.
     *
     * @throws Exception when K3PO is not started
     */
    @Test
    @Specification({
            "${scripts}/request.with.unknown.transfer.encoding/client",
            "${scripts}/request.with.unknown.transfer.encoding/server" })
    @ScriptProperty("serverTransport \"nukleus://http/streams/source\"")
    public void requestWithUnknownTransferEncoding() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_INPUT");
        k3po.finish();
    }

    /**
     * See <a href="https://tools.ietf.org/html/rfc7230#section-3">RFC 7230 section 3: Message Format</a>.
     *
     * @throws Exception when K3PO is not started
     */
    @Test
    @Specification({
            "${scripts}/post.request.with.no.content/client",
            "${scripts}/post.request.with.no.content/server" })
    @ScriptProperty("serverTransport \"nukleus://http/streams/source\"")
    public void postRequestWithNoContent() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_INPUT");
        k3po.finish();
    }

    /**
     * See <a href="https://tools.ietf.org/html/rfc7230#section-3">RFC 7230 section 3: Message Format</a>.
     *
     * Responses to the HEAD request method (Section 4.3.2 of [RFC7231]) never include a message body because the
     * associated response header fields (e.g., Transfer-Encoding, Content-Length, etc.), if present, indicate only what
     * their values would have been if the request method had been GET (Section 4.3.1 of [RFC7231]). 2xx (Successful)
     * responses to a CONNECT request method (Section 4.3.6 of [RFC7231]) switch to tunnel mode instead of having a
     * message body. All 1xx (Informational), 204 (No Content), and 304 (Not Modified) responses do not include a
     * message body.
     *
     * @throws Exception when K3PO is not started
     */
    @Test
    @Specification({
            "${scripts}/head.request.and.response/client",
            "${scripts}/head.request.and.response/server" })
    @ScriptProperty("serverTransport \"nukleus://http/streams/source\"")
    public void headRequestAndResponse() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_INPUT");
        k3po.finish();
    }

    /**
     * See <a href="https://tools.ietf.org/html/rfc7230#section-3">RFC 7230 section 3: Message Format</a>.
     *
     * Responses to the HEAD request method (Section 4.3.2 of [RFC7231]) never include a message body because the
     * associated response header fields (e.g., Transfer-Encoding, Content-Length, etc.), if present, indicate only what
     * their values would have been if the request method had been GET (Section 4.3.1 of [RFC7231]). 2xx (Successful)
     * responses to a CONNECT request method (Section 4.3.6 of [RFC7231]) switch to tunnel mode instead of having a
     * message body. All 1xx (Informational), 204 (No Content), and 304 (Not Modified) responses do not include a
     * message body.
     *
     * @throws Exception when K3PO is not started
     */
    @Test
    @Specification({
            "${scripts}/head.request.and.response.with.content.length/client",
            "${scripts}/head.request.and.response.with.content.length/server" })
    @ScriptProperty("serverTransport \"nukleus://http/streams/source\"")
    public void headRequestAndResponseWithContentLength() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_INPUT");
        k3po.finish();
    }

    /**
     * See <a href="https://tools.ietf.org/html/rfc7230#section-3.3.3">RFC 7230 section 3: Message Format</a>.
     *
     * If a message is received without Transfer-Encoding and with either multiple Content-Length header fields having
     * differing field-values or a single Content-Length header field having an invalid value, then the message framing
     * is invalid and the recipient MUST treat it as an unrecoverable error. If this is a request message, the server
     * MUST respond with a 400 (Bad Request) status code and then close the connection. If this is a response message
     * received by a proxy, the proxy MUST close the connection to the server, discard the received response, and send a
     * 502 (Bad Gateway) response to the client. If this is a response message received by a user agent, the user agent
     * MUST close the connection to the server and discard the received response.
     *
     * @throws Exception when K3PO is not started
     */
    @Test
    @Specification({
            "${scripts}/invalid.request.multiple.content.lengths/client",
            "${scripts}/invalid.request.multiple.content.lengths/server" })
    @ScriptProperty("serverTransport \"nukleus://http/streams/source\"")
    public void invalidRequestMultipleContentLengths() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_INPUT");
        k3po.finish();
    }

    /**
     * See <a href="https://tools.ietf.org/html/rfc7230#section-3.3.3">RFC 7230 section 3: Message Format</a>.
     *
     * If a message is received without Transfer-Encoding and with either multiple Content-Length header fields having
     * differing field-values or a single Content-Length header field having an invalid value, then the message framing
     * is invalid and the recipient MUST treat it as an unrecoverable error. If this is a request message, the server
     * MUST respond with a 400 (Bad Request) status code and then close the connection. If this is a response message
     * received by a proxy, the proxy MUST close the connection to the server, discard the received response, and send a
     * 502 (Bad Gateway) response to the client. If this is a response message received by a user agent, the user agent
     * MUST close the connection to the server and discard the received response.
     *
     * @throws Exception when K3PO is not started
     */
    @Test
    @Specification({
            "${scripts}/gateway.must.reject.request.with.multiple.different.content.length/client",
            "${scripts}/gateway.must.reject.request.with.multiple.different.content.length/gateway",
            "${scripts}/gateway.must.reject.request.with.multiple.different.content.length/server" })
    @ScriptProperty("serverTransport \"nukleus://http/streams/source\"")
    @Ignore("proxy tests not tests implemented")
    public void gatewayMustRejectResponseWithMultipleDifferentContentLength() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_INPUT");
        k3po.finish();
    }

    /**
     * See <a href="https://tools.ietf.org/html/rfc7230#section-3">RFC 7230 section 3: Message Format</a>.
     *
     * No whitespace is allowed between the header field-name and colon. In the past, differences in the handling of
     * such whitespace have led to security vulnerabilities in request routing and response handling. A server MUST
     * reject any received request message that contains whitespace between a header field-name and colon with a
     * response code of 400 (Bad Request). A proxy MUST remove any such whitespace from a response message before
     * forwarding the message downstream.
     *
     * @throws Exception when K3PO is not started
     */
    @Test
    @Specification({
            "${scripts}/on.response.proxy.must.remove.space.in.header.with.space.between.header.name.and.colon/client",
            "${scripts}/on.response.proxy.must.remove.space.in.header.with.space.between.header.name.and.colon/server",
            "${scripts}/on.response.proxy.must.remove.space.in.header.with.space.between.header.name.and.colon/proxy" })
    @ScriptProperty("serverTransport \"nukleus://http/streams/source\"")
    @Ignore("proxy tests not tests implemented")
    public void onResponseProxyMustRemoveSpaceInHeaderWithSpaceBetweenHeaderNameAndColon() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_INPUT");
        k3po.finish();
    }

    /**
     * See <a href="https://tools.ietf.org/html/rfc7230#section-3">RFC 7230 section 3: Message Format</a>.
     *
     * A proxy or gateway that receives an obs-fold in a response message that is not within a message/http container
     * MUST either discard the message and replace it with a 502 (Bad Gateway) response, preferably with a
     * representation explaining that unacceptable line folding was received, or replace each received obs-fold with one
     * or more SP octets prior to interpreting the field value or forwarding the message downstream.
     *
     * @throws Exception when K3PO is not started
     */
    @Test
    @Specification({
            "${scripts}/proxy.or.gateway.must.reject.obs.in.header.value/client",
            "${scripts}/proxy.or.gateway.must.reject.obs.in.header.value/server" })
    @ScriptProperty("serverTransport \"nukleus://http/streams/source\"")
    @Ignore("proxy tests not tests implemented")
    public void proxyOrGatewayMustRejectOBSInHeaderValue() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_INPUT");
        k3po.finish();
    }

    /**
     * See <a href="https://tools.ietf.org/html/rfc7230#section-3">RFC 7230 section 3: Message Format</a>.
     *
     * @throws Exception when K3PO is not started
     */
    @Test
    @Specification({
            "${scripts}/proxy.should.preserve.unrecongnized.headers/client",
            "${scripts}/proxy.should.preserve.unrecongnized.headers/server",
            "${scripts}/proxy.should.preserve.unrecongnized.headers/proxy" })
    @ScriptProperty("serverTransport \"nukleus://http/streams/source\"")
    @Ignore("proxy tests not tests implemented")
    public void proxyShouldPreserveUnrecognizedHeaders() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("ROUTED_INPUT");
        k3po.finish();
    }

}
