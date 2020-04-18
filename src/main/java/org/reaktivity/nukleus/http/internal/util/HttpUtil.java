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

import static java.lang.Character.toUpperCase;

import java.util.regex.Pattern;

import org.agrona.DirectBuffer;

public final class HttpUtil
{
    private final static byte ASCII_32 = 0x20;
    private final static byte ASCII_34 = 0x22;
    private final static byte ASCII_37 = 0x25;
    private final static byte ASCII_60 = 0x3C;
    private final static byte ASCII_62 = 0x3E;
    private final static byte ASCII_92 = 0x5C;
    private final static byte ASCII_94 = 0x5E;
    private final static byte ASCII_96 = 0x60;
    private final static byte ASCII_123 = 0x7B;
    private final static byte ASCII_124 = 0x7C;
    private final static byte ASCII_125 = 0x7D;
    private final static byte ASCII_127 = 0x7F;


    public static void appendHeader(
        StringBuilder payload,
        String name,
        String value)
    {
        StringBuilder initCapsName = new StringBuilder(name);
        int fromIndex = 0;
        do
        {
            initCapsName.setCharAt(fromIndex, toUpperCase(initCapsName.charAt(fromIndex)));
            fromIndex = initCapsName.indexOf("-", fromIndex) + 1;
        } while (fromIndex > 0 && fromIndex < initCapsName.length());

        payload.append(initCapsName).append(": ").append(value).append("\r\n");
    }

    public static boolean isValidPath(
        DirectBuffer path)
    {
        boolean isPathValid = true;
        for (int i = 0; i < path.capacity(); i ++)
        {
            byte charByte = path.getByte(i);

            if (((charByte & ASCII_32) == 0) ||
                ((charByte & 0x80) == 0x80) ||
                (charByte == ASCII_32) ||
                (charByte == ASCII_34) ||
                (charByte == ASCII_37) ||
                (charByte == ASCII_60) ||
                (charByte == ASCII_62) ||
                (charByte == ASCII_92) ||
                (charByte == ASCII_94) ||
                (charByte == ASCII_96) ||
                (charByte == ASCII_123) ||
                (charByte == ASCII_124) ||
                (charByte == ASCII_125) ||
                (charByte == ASCII_127))
            {
                isPathValid = false;
                break;
            }
        }
        return isPathValid;
    }


    private HttpUtil()
    {
        // utility class, no instances
    }
}
