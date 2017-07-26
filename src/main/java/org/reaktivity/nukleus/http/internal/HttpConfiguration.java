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
package org.reaktivity.nukleus.http.internal;

import org.reaktivity.nukleus.Configuration;

public class HttpConfiguration extends Configuration
{
    // Maximum number of parallel connections to a given target name and ref (i.e. route) when
    // the HTTP nukleus is acting as a client
    public static final String MAXIMUM_CONNECTIONS_PROPERTY_NAME = "nukleus.http.maximum.connections";

    private static final int MAXIMUM_CONNECTIONS_DEFAULT = 10; // most browsers use 6, IE 11 uses 13

    public HttpConfiguration(
        Configuration config)
    {
        super(config);
    }

    public int maximumConnectionsPerRoute()
    {
        return getInteger(MAXIMUM_CONNECTIONS_PROPERTY_NAME, MAXIMUM_CONNECTIONS_DEFAULT);
    }

}
