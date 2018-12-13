/**
 * Copyright 2016-2018 The Reaktivity Project
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
import org.reaktivity.reaktor.test.ReaktorRule;

public class TransferCodingsIT
{
    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("route", "org/reaktivity/specification/nukleus/http/control/route")
            .addScriptRoot("server", "org/reaktivity/specification/http/rfc7230/transfer.codings")
            .addScriptRoot("client", "org/reaktivity/specification/nukleus/http/streams/rfc7230/transfer.codings");

    private final TestRule timeout = new DisableOnDebug(new Timeout(10, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .nukleus("http"::equals)
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(4096)
        .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/request.transfer.encoding.chunked/client",
        "${server}/request.transfer.encoding.chunked/server" })
    @Ignore // TODO: implement chunked request encoding
    public void requestTransferEncodingChunked() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/response.transfer.encoding.chunked/client",
        "${server}/response.transfer.encoding.chunked/server" })
    public void responseTransferEncodingChunked() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Ignore("${scripts}/requires enhancement https://github.com/k3po/k3po/issues/313")
    public void requestTransferEncodingChunkedExtension() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Ignore("${scripts}/requires enhancement https://github.com/k3po/k3po/issues/313")
    public void responseTransferEncodingChunkedExtension() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/request.transfer.encoding.chunked.with.trailer/client",
        "${server}/request.transfer.encoding.chunked.with.trailer/server" })
    @Ignore // TODO: implement chunked request encoding
    public void requestTransferEncodingChunkedWithTrailer() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/response.transfer.encoding.chunked.with.trailer/client",
        "${server}/response.transfer.encoding.chunked.with.trailer/server" })
    @Ignore // TODO: implement chunked response trailer decoding
    public void responseTransferEncodingChunkedWithTrailer() throws Exception
    {
        k3po.finish();
    }

}
