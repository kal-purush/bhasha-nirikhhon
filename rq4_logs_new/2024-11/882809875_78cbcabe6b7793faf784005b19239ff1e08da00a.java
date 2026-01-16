package org.wecancodeit.backend.BOService;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.PathMatchConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import org.springframework.web.util.pattern.PathPatternParser;

/**
 * Configuration class for customizing the path matching behavior of the web application.
 * This class implements the {@link WebMvcConfigurer} interface to override 
 * and configure path-matching rules in the Spring MVC framework.
 */
@Configuration
public class WebConfig implements WebMvcConfigurer {

    /**
     * Configures the path matching options for the application.
     * This method sets the {@link PathPatternParser} to be case-insensitive when matching paths.
     * 
     * @param configurer the {@link PathMatchConfigurer} to be customized
     */
    @SuppressWarnings("null")
    @Override
    public void configurePathMatch(PathMatchConfigurer configurer) {
        // Create a new PathPatternParser instance
        var parser = new PathPatternParser();
        
        // Make path matching case-insensitive
        parser.setCaseSensitive(false);
        
        // Apply the custom parser to the PathMatchConfigurer
        configurer.setPatternParser(parser);
    }
}