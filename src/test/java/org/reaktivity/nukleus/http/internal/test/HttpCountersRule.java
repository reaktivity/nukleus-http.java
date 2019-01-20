/**
 * Copyright 2016-2018 The Reaktivity Project
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
package org.reaktivity.nukleus.http.internal.test;

import static org.junit.Assert.assertEquals;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.reaktivity.reaktor.test.ReaktorRule;

public class HttpCountersRule implements TestRule
{
    private final ReaktorRule reaktor;

    public HttpCountersRule(
        ReaktorRule reaktor)
    {
        this.reaktor = reaktor;
    }

    @Override
    public Statement apply(Statement base, Description description)
    {
        return new Statement()
        {

            @Override
            public void evaluate() throws Throwable
            {
                assertEquals(0, streams());
                assertEquals(0, routes());
                assertEquals(0, enqueues());
                assertEquals(0, dequeues());
                base.evaluate();
                assertEquals(enqueues(), dequeues());
            }

        };
    }

    public long routes()
    {
        return reaktor.counter("http.routes");
    }

    public long streams()
    {
        return reaktor.counter("http.streams");
    }

    public long enqueues()
    {
        return reaktor.counter("http.enqueues");
    }

    public long dequeues()
    {
        return reaktor.counter("http.dequeues");
    }

    public long requestsRejected()
    {
        return reaktor.counter("http.requests.rejected");
    }
}
