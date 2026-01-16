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
package com.redhat.console.notifications.splunkintegration;

import java.io.IOException;

import io.quarkus.runtime.annotations.RegisterForReflection;
import org.apache.camel.builder.endpoint.EndpointRouteBuilder;
import org.apache.camel.http.base.HttpOperationFailedException;
import org.apache.http.ProtocolException;

/**
 * Base class for all Integrations
 */

/*
 * We need to register some classes for reflection here, so that
 * native compilation can work if desired.
 */
@RegisterForReflection(targets = {
        Exception.class,
        HttpOperationFailedException.class,
        IOException.class
})
abstract class IntegrationsRouteBuilder extends EndpointRouteBuilder {
    @Override
    public void configure() throws Exception {
        configureErrorHandler();
    }

    protected void configureErrorHandler() throws Exception {
        onException(IOException.class)
                .to(direct("ioFailed"))
                .handled(true);
        onException(HttpOperationFailedException.class)
                .to(direct("httpFailed"))
                .handled(true);
        onException(IllegalArgumentException.class)
                .to(direct("targetUrlValidationFailed"))
                .handled(true);
        onException(ProtocolException.class)
                .to(direct("secureConnectionFailed"))
                .handled(true);
    }
}