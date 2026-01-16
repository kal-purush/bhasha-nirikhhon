package com.kapeta.spring.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kapeta.spring.dto.KapetaAuthenticationMetadata;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.web.SecurityFilterChain;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kapeta.spring.config.providers.KapetaConfigurationProvider;

@Configuration
@EnableWebSecurity
@EnableMethodSecurity
public class KapetaSecurityConsumerConfig {

    public static final String PATH_KAPETA_AUTHENTICATION = "/.kapeta/authentication.json";

    private final KapetaConfigurationProvider kapetaConfigurationProvider;
    private final ObjectMapper objectMapper;

    public KapetaSecurityConsumerConfig(KapetaConfigurationProvider kapetaConfigurationProvider, ObjectMapper objectMapper) {
        this.kapetaConfigurationProvider = kapetaConfigurationProvider;
        this.objectMapper = objectMapper;
    }

    @Bean
    public SecurityFilterChain oauthResourceServerFilterChain(HttpSecurity http) throws Exception {
        String baseUrl = kapetaConfigurationProvider.getServiceAddress("authjwtconsumer", "http");
        while (baseUrl != null && baseUrl.endsWith("/")) {
            baseUrl = baseUrl.substring(0, baseUrl.length() - 1);
        }

        String url = baseUrl + PATH_KAPETA_AUTHENTICATION;
        KapetaAuthenticationMetadata kapetaAuthenticationMetadata = fetchAndUnmarshal(url);
        String jwkSetUri = baseUrl + kapetaAuthenticationMetadata.getJwks();

        http
                .oauth2ResourceServer(oauth2 -> oauth2
                        .jwt(jwt -> jwt.jwkSetUri(jwkSetUri))
                );
        return http.build();
    }

    private KapetaAuthenticationMetadata fetchAndUnmarshal(String url) {
        String authenticationJson = fetch(url);
        try {
            return objectMapper.readValue(authenticationJson, KapetaAuthenticationMetadata.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private String fetch(String url) {
        HttpURLConnection connection = null;
        try {
            connection = (HttpURLConnection) new URL(url).openConnection();
            connection.setRequestMethod("GET");

            int responseCode = connection.getResponseCode();

            if (responseCode == HttpURLConnection.HTTP_OK) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
                    String line;
                    StringBuilder response = new StringBuilder();
                    while ((line = reader.readLine()) != null) response.append(line);
                    return response.toString();
                }
            } else {
                throw new RuntimeException("Unable to fetch KapetaAuthenticationMetadata. Reason=" + responseCode);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }
}