package com.sign.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sign.controller.config.ResponseProtectorProperties;
import com.sign.dto.AppToken;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import org.paseto4j.commons.PasetoException;
import org.paseto4j.commons.SecretKey;
import org.paseto4j.commons.Version;
import org.paseto4j.version2.Paseto;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Service;

@RequiredArgsConstructor
@EnableConfigurationProperties(ResponseProtectorProperties.class)
@Service
public class ResponseProtector {

    private final ResponseProtectorProperties properties;
    private final Clock clock;

    public String encrypt(Object value) {
        try {
            AppToken token = new AppToken(value, calculateExpired());
            String payload = mapper().writeValueAsString(token);
            return Paseto.encrypt(key(), payload, properties.footer());
        } catch (PasetoException | JsonProcessingException e) {
            e.printStackTrace();
            return "";
        }
    }

    private Instant calculateExpired() {
        return Instant.now(clock).plus(properties.expired());
    }

    private SecretKey key() {
        return new SecretKey(properties.secret().getBytes(StandardCharsets.UTF_8), Version.V2);
    }

    private JsonMapper mapper() {
        JsonMapper mapper = new JsonMapper();
        mapper.registerModule(new JavaTimeModule());
        return mapper;
    }
}