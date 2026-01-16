/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.redhat.console.integrations.servicenow;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.enterprise.context.ApplicationScoped;

import com.redhat.console.integrations.IntegrationsRouteBuilder;
import com.redhat.console.integrations.TargetUrlValidator;
import io.quarkus.runtime.annotations.RegisterForReflection;
import org.apache.camel.Exchange;
import org.apache.camel.component.http.HttpClientConfigurer;
import org.apache.camel.http.base.HttpOperationFailedException;
import org.apache.camel.http.common.HttpHeaderFilterStrategy;
import org.apache.camel.model.dataformat.JsonLibrary;
import org.apache.http.ProtocolException;

@RegisterForReflection(targets = {
        Exception.class,
        HttpOperationFailedException.class,
        IOException.class,
        ProtocolException.class
})
@ApplicationScoped
public class ServiceNowIntegration extends IntegrationsRouteBuilder {

    class ServiceNowHttpHeaderStrategy extends HttpHeaderFilterStrategy {
        @Override
        protected void initialize() {
            setLowerCase(true);
            setFilterOnMatch​(false); // reverse filtering to only accept selected

            getInFilter().clear();
            getOutFilter().clear();
            getOutFilter().add("authorization");
        }
    }

    @Override
    public void configure() throws Exception {
        super.configure();
        configureHandler();
    }

    private void configureHandler() throws Exception {
        from(direct("handler"))
                .routeId("handler")

                //Add headers useful for error reporting and metrics
                .setHeader("targetUrl", simple("${headers.metadata[url]}"))
                .setHeader("timeIn", simpleF("%d", System.currentTimeMillis()))

                // body is a JsonObject so converting to consumable object
                // for the http producer
                .marshal().json(JsonLibrary.Jackson)

                // validate the TargetUrl to be a proper url
                .process(new TargetUrlValidator())

                .setHeader(Exchange.HTTP_URI, header("targetUrl"))
                .to(https("dynamic")
                        .httpMethod("POST")
                        .headerFilterStrategy(new ServiceNowHttpHeaderStrategy())
                        .advanced()
                        .httpClientConfigurer(getClientConfigurer()))

                .to(direct("success"));
    }

    protected HttpClientConfigurer getClientConfigurer() {
        return (clientBuilder) -> {
            // proactively evict expired connections from the connection pool using a background thread
            clientBuilder.evictExpiredConnections();

            // proactively evict idle connections from the connection pool after 5s using a background thread.
            // Arguments set maximum time persistent connections can stay idle while kept alive in the connection pool.
            // Connections whose inactivity period exceeds this value will get closed and evicted from the pool.
            clientBuilder.evictIdleConnections(5L, TimeUnit.SECONDS);

        };
    }
}