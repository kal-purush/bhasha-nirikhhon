/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package com.example.greetingstream.impl;

import com.google.inject.AbstractModule;
import com.lightbend.lagom.javadsl.server.ServiceGuiceSupport;
import com.example.greeting.api.GreetingService;
import com.example.greetingstream.api.GreetingStreamService;

/**
 * The module that binds the GreetingStreamService so that it can be served.
 */
public class GreetingStreamModule extends AbstractModule implements ServiceGuiceSupport {
  @Override
  protected void configure() {
    // Bind the GreetingStreamService service
    bindServices(serviceBinding(GreetingStreamService.class, GreetingStreamServiceImpl.class));
    // Bind the GreetingService client
    bindClient(GreetingService.class);
  }
}