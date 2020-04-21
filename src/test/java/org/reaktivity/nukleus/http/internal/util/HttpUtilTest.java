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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

public class HttpUtilTest
{
    @Test
    public void shouldAppendHeader()
    {
        StringBuilder message = new StringBuilder("...");
        HttpUtil.appendHeader(message, "Header", "header-value");
        assertEquals("...Header: header-value\r\n", message.toString());
    }

    @Test
    public void shouldInitCapInitialHeaderNameCharacter()
    {
        StringBuilder message = new StringBuilder();
        HttpUtil.appendHeader(message, "host", "value");
        assertEquals("Host: value\r\n", message.toString());
    }

    @Test
    public void shouldInitCapCharacterFollowingHyphen()
    {
        StringBuilder message = new StringBuilder();
        HttpUtil.appendHeader(message, "content-length", "14");
        assertEquals("Content-Length: 14\r\n", message.toString());
    }

    @Test
    public void shouldInitCapCharacterFollowingHyphenWithHyphenAtStart()
    {
        StringBuilder message = new StringBuilder();
        HttpUtil.appendHeader(message, "-name", "value");
        assertEquals("-Name: value\r\n", message.toString());
    }

    @Test
    public void shouldHandleHaderNameWithHyphenAtEnd()
    {
        StringBuilder message = new StringBuilder();
        HttpUtil.appendHeader(message, "name-", "value");
        assertEquals("Name-: value\r\n", message.toString());
    }

    @Test
    public void shouldHandleSingleHyhenHeaderName()
    {
        StringBuilder message = new StringBuilder();
        HttpUtil.appendHeader(message, "-", "value");
        assertEquals("-: value\r\n", message.toString());
    }

    @Test
    public void shouldHandleAllHyphensHeaderName()
    {
        StringBuilder message = new StringBuilder();
        HttpUtil.appendHeader(message, "---", "value");
        assertEquals("---: value\r\n", message.toString());
    }

    @Test
    public void shouldAcceptValidPath()
    {
        String path1 = "/api/invalid?limit=10000&offset=0&geometry=";
        String path2 = "/pathof8";
        String path3 = "/pathof09";
        String path4 = "/pathof0000000017";
        assertTrue(HttpUtil.isPathValid(new UnsafeBuffer(path1.getBytes())));
        assertTrue(HttpUtil.isPathValid(new UnsafeBuffer(path2.getBytes())));
        assertTrue(HttpUtil.isPathValid(new UnsafeBuffer(path3.getBytes())));
        assertTrue(HttpUtil.isPathValid(new UnsafeBuffer(path4.getBytes())));
    }

    @Test
    public void shouldRejectInvalidPath()
    {
        String path = "/api/invalid?limit=10000&offset=0&geometry={[[[-1,0],[-3,4]]}";
        assertFalse(HttpUtil.isPathValid(new UnsafeBuffer(path.getBytes())));
    }

}

