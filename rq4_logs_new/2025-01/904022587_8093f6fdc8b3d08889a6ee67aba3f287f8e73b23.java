package com.green.util;

import java.io.IOException;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import jakarta.annotation.PreDestroy;

@Configuration
public class AsyncHttpClientConfig {

    private AsyncHttpClient asyncHttpClient;

    @Bean
    public AsyncHttpClient asyncHttpClient() {
        this.asyncHttpClient = new DefaultAsyncHttpClient();
        return this.asyncHttpClient;
    }

    @PreDestroy
    public void destroy() throws IOException {
        if (asyncHttpClient != null) {
            asyncHttpClient.close();
        }
    }
}