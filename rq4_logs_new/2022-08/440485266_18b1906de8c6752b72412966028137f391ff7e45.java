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

import javax.enterprise.context.ApplicationScoped;

import org.apache.camel.LoggingLevel;
import org.apache.camel.Processor;
import org.apache.camel.builder.endpoint.EndpointRouteBuilder;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Configures Error Routes
 */

@ApplicationScoped
public class ErrorHandlingRoutes extends EndpointRouteBuilder {

    private static final Config CONFIG = ConfigProvider.getConfig();

    // The name of our component. Must be unique
    public static final String COMPONENT_NAME = CONFIG.getValue("integrations.component.name", String.class);

    // The return type
    public static final String RETURN_TYPE = "com.redhat.console.notifications.history";

    @Override
    public void configure() throws Exception {
        configureIoFailed();
        configureHttpFailed();
        configureTargetUrlValidationFailed();
        configureSecureConnectionFailed();
    }

    private void configureSecureConnectionFailed() throws Exception {
        Processor ceEncoder = new CloudEventEncoder(COMPONENT_NAME, RETURN_TYPE);
        Processor resultTransformer = new ResultTransformer();
        // The error handler when we receive an HTTP (unsecure) connection instead of HTTPS
        from(direct("secureConnectionFailed"))
                .routeId("secureConnectionFailed")
                .log(LoggingLevel.ERROR, "ProtocolException for event ${header.ce-id} (orgId ${header.orgId}"
                                         + " account ${header.accountId}) to ${header.targetUrl}: ${exception.message}")
                .log(LoggingLevel.DEBUG, "${exception.stacktrace}")
                .setBody(simple("${exception.message}"))
                .setHeader("outcome-fail", simple("true"))
                .process(resultTransformer)
                .marshal().json()
                .process(ceEncoder)
                .to(direct("return"));
    }

    private void configureTargetUrlValidationFailed() throws Exception {
        Processor ceEncoder = new CloudEventEncoder(COMPONENT_NAME, RETURN_TYPE);
        Processor resultTransformer = new ResultTransformer();
        // The error handler when we receive a TargetUrlValidator failure
        from(direct("targetUrlValidationFailed"))
                .routeId("targetUrlValidationFailed")
                .log(LoggingLevel.ERROR, "IllegalArgumentException for event ${header.ce-id} (orgId ${header.orgId}"
                                         + " account ${header.accountId}) to ${header.targetUrl}: ${exception.message}")
                .log(LoggingLevel.DEBUG, "${exception.stacktrace}")
                .setBody(simple("${exception.message}"))
                .setHeader("outcome-fail", simple("true"))
                .process(resultTransformer)
                .marshal().json()
                .process(ceEncoder)
                .to(direct("return"));
    }

    private void configureIoFailed() throws Exception {
        Processor ceEncoder = new CloudEventEncoder(COMPONENT_NAME, RETURN_TYPE);
        Processor resultTransformer = new ResultTransformer();
        // The error handler found an IO Exception. We set the outcome to fail and then send to kafka
        from(direct("ioFailed"))
                .routeId("ioFailed")
                .log(LoggingLevel.ERROR, "IOFailure for event ${header.ce-id} (orgId ${header.orgId}"
                                         + " account ${header.accountId}) to ${header.targetUrl}: ${exception.message}")
                .log(LoggingLevel.DEBUG, "${exception.stacktrace}")
                .setBody(simple("${exception.message}"))
                .setHeader("outcome-fail", simple("true"))
                .process(resultTransformer)
                .marshal().json()
                .process(ceEncoder)
                .to(direct("return"));
    }

    private void configureHttpFailed() throws Exception {
        Processor ceEncoder = new CloudEventEncoder(COMPONENT_NAME, RETURN_TYPE);
        Processor resultTransformer = new ResultTransformer();
        // The error handler found an HTTP Exception. We set the outcome to fail and then send to kafka
        from(direct("httpFailed"))
                .routeId("httpFailed")
                .log(LoggingLevel.ERROR, "HTTPFailure for event ${header.ce-id} (orgId ${header.orgId} account"
                                         + " ${header.accountId}) to ${header.targetUrl}: ${exception.getStatusCode()}"
                                         + " ${exception.getStatusText()}: ${exception.message}")
                .log(LoggingLevel.DEBUG, "Response Body: ${exception.getResponseBody()}")
                .log(LoggingLevel.DEBUG, "Response Headers: ${exception.getResponseHeaders()}")
                .setBody(simple("${exception.message}"))
                .setHeader("outcome-fail", simple("true"))
                .process(resultTransformer)
                .marshal().json()
                .process(ceEncoder)
                .to(direct("return"));
    }
}