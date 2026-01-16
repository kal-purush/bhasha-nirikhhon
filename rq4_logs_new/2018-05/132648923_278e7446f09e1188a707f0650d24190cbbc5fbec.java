package com.opentable.components;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * A Spring Configuration class for our Component.
 * Inject configuration values, set up your beans, etc.
 */
@Configuration
public class ComponentConfiguration {
    private static final Logger LOG = LoggerFactory.getLogger(ComponentConfiguration.class);

    @Value("${ot.component.config:#{null}}")
    private String configurable;

    @Bean
    public Component component() {
        LOG.info("Initializing Component with configurable {}", configurable);
        return new Component(configurable);
    }
}