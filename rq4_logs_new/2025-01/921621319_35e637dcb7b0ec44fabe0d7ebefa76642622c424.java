package com.metamong.mt.global.web.cookie;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.http.ResponseCookie;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnMissingBean(CookieGenerator.class)
public class DefaultCookieGenerator implements CookieGenerator {
    private final String clientDomain;
    
    public DefaultCookieGenerator(@Value("${client.domain}") String clientDomain) {
        this.clientDomain = clientDomain;
    }

    @Override
    public ResponseCookie generateCookie(String name, String value) {
        return ResponseCookie.from(name, value)
                .httpOnly(true)
                .path(CookieGenerator.PATH)
                .maxAge(CookieGenerator.MAX_AGE)
                .domain(this.clientDomain)
                .secure(true)
                .build();
    }
}