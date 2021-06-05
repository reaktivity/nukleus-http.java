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
package org.reaktivity.nukleus.http2.internal.config;

import javax.json.JsonObject;
import javax.json.bind.adapter.JsonbAdapter;

import org.reaktivity.nukleus.http.internal.config.HttpConditionAdapter;
import org.reaktivity.nukleus.http2.internal.Http2Nukleus;
import org.reaktivity.reaktor.config.Condition;
import org.reaktivity.reaktor.config.ConditionAdapterSpi;

public final class Http2ConditionAdapter implements ConditionAdapterSpi, JsonbAdapter<Condition, JsonObject>
{
    private final HttpConditionAdapter delegate = new HttpConditionAdapter();

    @Override
    public String type()
    {
        return Http2Nukleus.NAME;
    }

    @Override
    public JsonObject adaptToJson(
        Condition condition)
    {
        return delegate.adaptToJson(condition);
    }

    @Override
    public Condition adaptFromJson(
        JsonObject object)
    {
        return delegate.adaptFromJson(object);
    }
}
