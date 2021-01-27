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
package org.reaktivity.nukleus.http.internal;

import static org.junit.Assert.assertEquals;
import static org.reaktivity.nukleus.http.internal.HttpConfiguration.HTTP_MAXIMUM_QUEUED_REQUESTS;

import org.junit.Test;

public class HttpConfigurationTest
{
    // needed by test annotations
    public static final String HTTP_MAXIMUM_QUEUED_REQUESTS_NAME = "nukleus.http.maximum.requests.queued";

    @Test
    public void shouldVerifyConstants() throws Exception
    {
        assertEquals(HTTP_MAXIMUM_QUEUED_REQUESTS.name(), HTTP_MAXIMUM_QUEUED_REQUESTS_NAME);
    }
}
