package com.deo.microservices.smsservice.config;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties("twilio")
@Getter
@Setter
@RequiredArgsConstructor
public class TwilioConfig {
    private String accountSid;
    private String authToken;
    private String phoneNumber;
}