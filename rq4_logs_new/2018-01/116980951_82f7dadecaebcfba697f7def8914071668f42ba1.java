package com.lohika.morning.ecs;

import lombok.extern.slf4j.Slf4j;
import microsoft.exchange.webservices.data.autodiscover.exception.AutodiscoverLocalException;
import microsoft.exchange.webservices.data.core.ExchangeService;
import microsoft.exchange.webservices.data.core.enumeration.misc.ExchangeVersion;
import microsoft.exchange.webservices.data.credential.ExchangeCredentials;
import microsoft.exchange.webservices.data.credential.WebCredentials;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;

@Configuration
@Slf4j
public class ExchangeConfiguration {

    @Value("${exchange.username}")
    private String user;

    @Value("${exchange.password}")
    private String pwd;

    @Value("${exchange.trace.enabled}")
    private boolean exchangeTraceEnabled;

    @Value("${exchange.default-url}")
    private String exchangeServiceDefaultUrl;

    @Value("${exchange.autodiscovery.enabled}")
    private boolean exchangeAutodiscoveryEnabled;

    @Value("${spring.config.location}")
    private String scl;

    @Bean
    public ExchangeService exchangeService() throws Exception {
        ExchangeService service = new ExchangeService(ExchangeVersion.Exchange2010_SP2);

        ExchangeCredentials credentials = new WebCredentials(user, pwd);
        service.setCredentials(credentials);

        service.setTraceListener((traceType, traceMessage) -> log.info("Type: {}; Message: {}", traceType, traceMessage));
        service.setTraceEnabled(exchangeTraceEnabled);

        service.setUrl(new URI(exchangeServiceDefaultUrl));

        if (exchangeAutodiscoveryEnabled) {
            log.info("Auto-discovering exchange service URL...");

            try {
                service.autodiscoverUrl(user, url -> {
                    log.info("Autodiscovered exchange service URL: {}", url.toLowerCase());
                    return url.toLowerCase().startsWith("https://");
                });
            } catch (AutodiscoverLocalException e) {
                log.warn("Exchange service autodiscovery failed. Using default exchange service URL");
            }
        }

        log.info("Final exchange service URL: {}", service.getUrl());

        return service;
    }
}