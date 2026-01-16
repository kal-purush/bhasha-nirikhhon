package com.example.rseservice.config.feign;

import com.example.rseservice.config.feign.decoder.CgiErrorDecoder;
import feign.Feign;
import feign.Logger;
import feign.Retryer;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FeignConfig {

    private static Integer CONNECTION_TIMEOUT = 10000;
    private static Integer READ_TIMEOUT = 60000;

    private String cgiUrl = "http://localhost:8000";

    @Bean
    public CGIApi cgiFeign() {
        return Feign.builder()
                .encoder(new JacksonEncoder())
                .decoder(new JacksonDecoder())
                .logLevel(Logger.Level.FULL)
                .retryer(Retryer.NEVER_RETRY)
                .errorDecoder(new CgiErrorDecoder())
                .target(CGIApi.class, cgiUrl);
    }


}