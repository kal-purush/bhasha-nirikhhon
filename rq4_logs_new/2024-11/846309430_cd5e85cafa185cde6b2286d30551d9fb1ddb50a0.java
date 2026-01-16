package com.example.support.component;

import com.example.support.configuration.CacheConfig;
import java.util.Map;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.security.oauth2.core.DefaultOAuth2AuthenticatedPrincipal;
import org.springframework.security.oauth2.core.OAuth2AuthenticatedPrincipal;
import org.springframework.security.oauth2.server.resource.introspection.OAuth2IntrospectionException;
import org.springframework.security.oauth2.server.resource.introspection.OpaqueTokenIntrospector;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
public class CachingOpaqueTokenIntrospector implements OpaqueTokenIntrospector {

    private final OpaqueTokenIntrospector opaqueTokenIntrospector;

    @Cacheable(value = CacheConfig.MEMBER, key = "#token")
    @Override
    public OAuth2AuthenticatedPrincipal introspect(String token) {
        try {
            OAuth2AuthenticatedPrincipal response = opaqueTokenIntrospector.introspect(token);

            if (Objects.equals(response.getAttribute("active"), false)) {
                throw new OAuth2IntrospectionException("Token is not active");
            }

            Map<String, Object> claims = response.getAttributes();
            return new DefaultOAuth2AuthenticatedPrincipal(claims, null);
        } catch (Exception ex) {
            throw new OAuth2IntrospectionException("Failed to introspect token", ex);
        }
    }
}