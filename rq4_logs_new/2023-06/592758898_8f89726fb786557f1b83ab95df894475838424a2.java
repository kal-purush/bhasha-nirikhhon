package org.ua.javaPro.berezhnoy.bank.config;


import com.github.tomakehurst.wiremock.WireMockServer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class WiremockConfig {
    private static final WireMockServer wireMockServer = new WireMockServer(8089);

    @Bean
    public static WireMockServer getWireMockServer() {
        return wireMockServer;
    }

    @PostConstruct
    public void startWireMockServer() {
        getWireMockServer().start();
    }

    @PreDestroy
    public void stopWireMockServer() {
        getWireMockServer().stop();
    }

}