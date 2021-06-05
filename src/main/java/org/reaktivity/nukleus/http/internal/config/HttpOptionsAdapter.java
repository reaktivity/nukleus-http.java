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
package org.reaktivity.nukleus.http.internal.config;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonString;
import javax.json.bind.adapter.JsonbAdapter;

import org.reaktivity.nukleus.http.internal.HttpNukleus;
import org.reaktivity.nukleus.http.internal.types.String16FW;
import org.reaktivity.nukleus.http.internal.types.String8FW;
import org.reaktivity.reaktor.config.Options;
import org.reaktivity.reaktor.config.OptionsAdapterSpi;

public final class HttpOptionsAdapter implements OptionsAdapterSpi, JsonbAdapter<Options, JsonObject>
{
    private static final String OVERRIDES_NAME = "overrides";

    @Override
    public String type()
    {
        return HttpNukleus.NAME;
    }

    @Override
    public JsonObject adaptToJson(
        Options options)
    {
        HttpOptions httpOptions = (HttpOptions) options;

        JsonObjectBuilder object = Json.createObjectBuilder();

        if (httpOptions.overrides != null &&
            !httpOptions.overrides.isEmpty())
        {
            JsonObjectBuilder entries = Json.createObjectBuilder();
            httpOptions.overrides.forEach((k, v) -> entries.add(k.asString(), v.asString()));

            object.add(OVERRIDES_NAME, entries);
        }

        return object.build();
    }

    @Override
    public Options adaptFromJson(
        JsonObject object)
    {
        JsonObject overrides = object.containsKey(OVERRIDES_NAME)
                ? object.getJsonObject(OVERRIDES_NAME)
                : null;

        Map<String8FW, String16FW> newOverrides = null;

        if (overrides != null)
        {
            Map<String8FW, String16FW> newOverrides0 = new LinkedHashMap<>();
            overrides.forEach((k, v) ->
                newOverrides0.put(new String8FW(k), new String16FW(JsonString.class.cast(v).getString())));
            newOverrides = newOverrides0;
        }

        return new HttpOptions(newOverrides);
    }
}
