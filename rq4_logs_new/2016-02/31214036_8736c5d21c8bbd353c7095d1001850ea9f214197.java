package com.peterphi.std.guice.common;

import com.google.inject.AbstractModule;
import com.peterphi.std.guice.common.introspective.IntrospectiveInfoRegistry;
import org.apache.commons.configuration.Configuration;

public class IntrospectiveModule extends AbstractModule
{
    private final Configuration config;

    public IntrospectiveModule(Configuration config)
    {
        this.config = config;
    }

    @Override
    protected void configure() {
        bind(IntrospectiveInfoRegistry.class);
    }
}