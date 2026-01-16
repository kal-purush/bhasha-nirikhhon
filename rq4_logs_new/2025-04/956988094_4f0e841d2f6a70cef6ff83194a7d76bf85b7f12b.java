package com.prime.utils;

import lombok.experimental.UtilityClass;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.util.ObjectUtils;

import java.util.Objects;
import java.util.UUID;

@UtilityClass
public class SecurityUtil {

    public Object getPrincipal() {
        return SecurityContextHolder.getContext().getAuthentication().getPrincipal();
    }

    public Object getDetails() {
        return SecurityContextHolder.getContext().getAuthentication().getDetails();
    }

    public Object getCredentials() {
        return SecurityContextHolder.getContext().getAuthentication().getCredentials();
    }

    public UUID getIDUser() {
        return UUID.fromString(Objects.requireNonNull(getValueByKey("id")));
    }

    public String getUserName() {
        return getValueByKey("user");
    }

    private String getValueByKey(String key) {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();

        if (authentication instanceof JwtAuthenticationToken jwtAuthToken) {
            Object value = jwtAuthToken.getTokenAttributes().get(key);
            return !ObjectUtils.isEmpty(value) ? value.toString() : null;
        }

        return null;
    }


}