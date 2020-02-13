/**
 * Copyright 2016-2020 The Reaktivity Project
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
package org.reaktivity.nukleus.http.internal.util;

public final class HttpUtil
{
    public static void appendHeader(
        StringBuilder payload,
        String name,
        String value)
    {
        StringBuilder initCapsName = new StringBuilder(name);
        int fromIndex = 0;
        do
        {
            initCapsName.setCharAt(fromIndex, Character.toUpperCase(initCapsName.charAt(fromIndex + 1)));
            fromIndex =  initCapsName.indexOf("-", fromIndex);
        } while (fromIndex > 0 && fromIndex < initCapsName.length());

        payload.append(initCapsName).append(": ").append(value).append("\r\n");
    }

    private HttpUtil()
    {
        // utility class, no instances
    }
}
