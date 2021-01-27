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
package org.reaktivity.nukleus.http2.internal.stream;

public interface Http2Flags
{
    byte NONE = 0x0;
    byte END_STREAM = 0x01;
    byte ACK = 0x01;
    byte END_HEADERS = 0x04;
    byte PADDED = 0x08;
    byte PRIORITY = 0x20;

    static boolean padded(byte flags)
    {
        return (flags & PADDED) != 0;
    }

    static boolean endStream(byte flags)
    {
        return (flags & END_STREAM) != 0;
    }

    static boolean endHeaders(byte flags)
    {
        return (flags & END_HEADERS) != 0;
    }

    static boolean priority(byte flags)
    {
        return (flags & PRIORITY) != 0;
    }

    static boolean ack(byte flags)
    {
        return (flags & ACK) != 0;
    }
}
