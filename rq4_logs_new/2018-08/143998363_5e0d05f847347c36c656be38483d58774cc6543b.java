package com.condescendors.pollingapp.pollingapp.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@ConfigurationProperties(prefix = "application", ignoreUnknownFields = false)
@Configuration
public class ApplicationProperties {

    private final ApplicationProperties.Security security = new ApplicationProperties.Security();

    public ApplicationProperties.Security getSecurity() {
        return this.security;
    }

    public ApplicationProperties() {
    }

    public static class Security {

        private final ApplicationProperties.Security.Authentication authentication = new ApplicationProperties.Security.Authentication();

        public ApplicationProperties.Security.Authentication getAuthentication() {
            return this.authentication;
        }

        public static class Authentication {

            private final ApplicationProperties.Security.Authentication.Jwt jwt = new ApplicationProperties.Security.Authentication.Jwt();

            public ApplicationProperties.Security.Authentication.Jwt getJwt() {
                return this.jwt;
            }

            public static class Jwt {

                private String secret;

                private long tokenValidityInSeconds;

                private long tokenValidityInSecondsForRememberMe;

                public String getSecret() {
                    return secret;
                }

                public void setSecret(String secret) {
                    this.secret = secret;
                }

                public long getTokenValidityInSeconds() {
                    return tokenValidityInSeconds;
                }

                public void setTokenValidityInSeconds(long tokenValidityInSeconds) {
                    this.tokenValidityInSeconds = tokenValidityInSeconds;
                }

                public long getTokenValidityInSecondsForRememberMe() {
                    return tokenValidityInSecondsForRememberMe;
                }

                public void setTokenValidityInSecondsForRememberMe(long tokenValidityInSecondsForRememberMe) {
                    this.tokenValidityInSecondsForRememberMe = tokenValidityInSecondsForRememberMe;
                }
            }
        }
    }
}