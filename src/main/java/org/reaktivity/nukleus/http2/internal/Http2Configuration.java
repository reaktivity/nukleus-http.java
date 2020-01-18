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
package org.reaktivity.nukleus.http2.internal;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.reaktor.ReaktorConfiguration;

public class Http2Configuration extends Configuration
{
    public static final boolean DEBUG_HTTP2_BUDGETS =
            ReaktorConfiguration.DEBUG_BUDGETS || Boolean.getBoolean("nukleus.http2.debug.budgets");

    public static final IntPropertyDef HTTP2_SERVER_CONCURRENT_STREAMS;
    public static final IntPropertyDef HTTP2_MAX_CLEANUP_STREAMS;
    public static final IntPropertyDef HTTP2_MAX_CONCURRENT_APPLICATION_HEADERS;
    public static final BooleanPropertyDef HTTP2_ACCESS_CONTROL_ALLOW_ORIGIN;
    public static final PropertyDef<String> HTTP2_SERVER_HEADER;

    private static final ConfigurationDef HTTP2_CONFIG;

    static
    {
        final ConfigurationDef config = new ConfigurationDef("nukleus.http2");
        HTTP2_SERVER_CONCURRENT_STREAMS = config.property("server.concurrent.streams", Integer.MAX_VALUE);
        HTTP2_ACCESS_CONTROL_ALLOW_ORIGIN = config.property("server.access.control.allow.origin", false);
        HTTP2_SERVER_HEADER = config.property("server.header");
        HTTP2_MAX_CLEANUP_STREAMS = config.property("max.cleanup.streams", 1000);
        HTTP2_MAX_CONCURRENT_APPLICATION_HEADERS = config.property("max.concurrent.application.headers", 10000);
        HTTP2_CONFIG = config;
    }

    private final DirectBuffer serverHeader;

    public Http2Configuration(
        Configuration config)
    {
        super(HTTP2_CONFIG, config);
        String server = HTTP2_SERVER_HEADER.get(this);
        serverHeader = server != null ? new UnsafeBuffer(server.getBytes(UTF_8)) : null;
    }

    public int serverConcurrentStreams()
    {
        return HTTP2_SERVER_CONCURRENT_STREAMS.getAsInt(this);
    }
    public int maxCleanupStreams()
    {
        return HTTP2_MAX_CLEANUP_STREAMS.getAsInt(this);
    }
    public int maxConcurrentApplicationHeaders()
    {
        return HTTP2_MAX_CONCURRENT_APPLICATION_HEADERS.getAsInt(this);
    }

    public boolean accessControlAllowOrigin()
    {
        return HTTP2_ACCESS_CONTROL_ALLOW_ORIGIN.get(this);
    }

    public DirectBuffer serverHeader()
    {
        return serverHeader;
    }

}
