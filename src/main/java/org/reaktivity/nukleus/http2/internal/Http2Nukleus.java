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
package org.reaktivity.nukleus.http2.internal;

import org.reaktivity.reaktor.nukleus.ElektronContext;
import org.reaktivity.reaktor.nukleus.Nukleus;

public final class Http2Nukleus implements Nukleus
{
    public static final String NAME = "http2";

    private final Http2Configuration config;

    Http2Nukleus(
        Http2Configuration config)
    {
        this.config = config;
    }

    @Override
    public String name()
    {
        return Http2Nukleus.NAME;
    }

    @Override
    public Http2Configuration config()
    {
        return config;
    }

    @Override
    public Http2Elektron supplyElektron(
        ElektronContext context)
    {
        return new Http2Elektron(config, context);
    }
}
