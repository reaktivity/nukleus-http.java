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
package org.reaktivity.nukleus.http.internal.util;

import static org.junit.Assert.assertEquals;

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

}

