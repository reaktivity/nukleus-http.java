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

import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.function.Function;

import org.reaktivity.reaktor.config.Binding;
import org.reaktivity.reaktor.config.Role;

public final class HttpBinding
{
    public final long id;
    public final long vaultId;
    public final String entry;
    public final HttpOptions options;
    public final Role kind;
    public final List<HttpRoute> routes;
    public final HttpRoute exit;

    public HttpBinding(
        Binding binding)
    {
        this.id = binding.id;
        this.vaultId = binding.vault != null ? binding.vault.id : 0L;
        this.entry = binding.entry;
        this.kind = binding.kind;
        this.options = HttpOptions.class.cast(binding.options);
        this.routes = binding.routes.stream().map(HttpRoute::new).collect(toList());
        this.exit = binding.exit != null ? new HttpRoute(binding.exit) : null;
    }

    public HttpRoute resolve(
        long authorization,
        Function<String, String> headerByName)
    {
        return routes.stream()
            .filter(r -> r.when.stream().allMatch(m -> m.matches(headerByName)))
            .findFirst()
            .orElse(exit);
    }
}
