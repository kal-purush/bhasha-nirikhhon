package com.appcharge.server.controller.auth.methods.impl;

import com.appcharge.server.controller.auth.methods.AppleAuth;
import com.appcharge.server.models.auth.AuthResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Base64;

@Service
public class AppleAuthImpl implements AppleAuth {
    public AuthResult authenticate(String appId, String token, String appleSecretApi) {
        if (appleSecretApi == null) {
            System.out.println("Apple secret API is not provided.");
            return new AuthResult(false, null);
        }

        String url = "https://appleid.apple.com/auth/token";

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/x-www-form-urlencoded");

        String requestBody = "client_id=" + appId
                + "&client_secret=" + appleSecretApi
                + "&code=" + token
                + "&grant_type=authorization_code";

        requestBuilder.POST(HttpRequest.BodyPublishers.ofString(requestBody));

        try {
            HttpResponse<String> response = client.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                String responseString = response.body();
                ObjectMapper mapper = new ObjectMapper();
                JsonNode data = mapper.readTree(responseString);

                String accessToken = data.get("access_token").asText();
                String idToken = data.get("id_token").asText();

                String userId = extractUserIdFromIdToken(idToken);

                return new AuthResult(true, userId);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return new AuthResult(false, null);
    }

    private static String extractUserIdFromIdToken(String idToken) {
        String[] parts = idToken.split("\\.");
        if (parts.length != 3) {
            return null; // Invalid ID token format
        }

        String encodedPayload = parts[1];
        byte[] decodedPayload = Base64.getUrlDecoder().decode(encodedPayload);
        String payloadJson = new String(decodedPayload);

        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode payload = mapper.readTree(payloadJson);
            String userId = payload.get("sub").asText();
            return userId;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }
}