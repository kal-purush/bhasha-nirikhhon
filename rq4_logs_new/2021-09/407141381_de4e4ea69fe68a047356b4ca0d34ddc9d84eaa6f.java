package com.lucamusic.order;

import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

@Configuration
public class RestTemplateConfig {
    /*
     * We can register RestTemplate as a Spring bean with @LoadBalanced annotation.
     * The RestTemplate with @LoadBalanced annotation will internally use Ribbon LoadBalancer
     * to resolve the ServiceID and invoke REST endpoint using one of the available servers.
     */
    @Bean
    @LoadBalanced
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }
}