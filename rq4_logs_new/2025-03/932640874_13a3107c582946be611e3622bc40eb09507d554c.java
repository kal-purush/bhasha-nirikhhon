package dev.frilly.locket.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Configuration file for Jackson's JSON formats.
 */
@Configuration
public class JacksonConfig {

    /**
     * The Jackson's Object Mapper, after registering the Time Module
     * and disabling timestamp dates.
     *
     * @return the object mapper bean
     */
    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

}