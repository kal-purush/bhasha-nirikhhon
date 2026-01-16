package org.citrusframework.remote.sample.config;

import org.citrusframework.dsl.endpoint.CitrusEndpoints;
import org.citrusframework.http.client.HttpClient;
import org.citrusframework.http.server.HttpServer;
import org.springframework.context.annotation.Bean;

import java.time.Duration;

public class Http {
    private static final int SERVER_PORT = 8081;

    @Bean
    public HttpClient httpClient() {
        return CitrusEndpoints.http()
            .client()
            .requestUrl("http://localhost:%d".formatted(SERVER_PORT))
            .build();
    }

    @Bean
    public HttpServer httpServer() {
        return CitrusEndpoints.http()
            .server()
            .port(SERVER_PORT)
            .timeout(Duration.ofSeconds(10).toMillis())
            .autoStart(true)
            .build();
    }
}