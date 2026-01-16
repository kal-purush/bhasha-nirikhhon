package com.notificationapi;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.notificationapi.exception.NotificationApiException;
import com.notificationapi.model.NotificationRequest;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.*;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Main client class for interacting with the NotificationAPI service.
 */
public class NotificationApi implements AutoCloseable {
    private static final String BASE_URL = "https://api.notificationapi.com";
    private final String clientId;
    private final String clientSecret;
    private final String authToken;
    private final CloseableHttpClient httpClient;
    private final ObjectMapper objectMapper;

    /**
     * Constructs a new NotificationApi client.
     *
     * @param clientId your NotificationAPI client ID
     * @param clientSecret your NotificationAPI client secret
     * @throws IllegalArgumentException if clientId or clientSecret is null or empty
     */
    public NotificationApi(String clientId, String clientSecret) {
        if (clientId == null || clientId.trim().isEmpty()) {
            throw new IllegalArgumentException("clientId cannot be null or empty");
        }
        if (clientSecret == null || clientSecret.trim().isEmpty()) {
            throw new IllegalArgumentException("clientSecret cannot be null or empty");
        }

        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.authToken = Base64.getEncoder().encodeToString(
            (clientId + ":" + clientSecret).getBytes(StandardCharsets.UTF_8)
        );
        this.httpClient = HttpClients.createDefault();
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Sends a notification using the specified request parameters.
     *
     * @param request the notification request containing all necessary information
     * @return the response from the API as a string
     * @throws NotificationApiException if there is an error sending the notification
     */
    public String send(NotificationRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("request cannot be null");
        }
        if (request.getNotificationId() == null || request.getNotificationId().trim().isEmpty()) {
            throw new IllegalArgumentException("notificationId cannot be null or empty");
        }
        if (request.getUser() == null || request.getUser().getId() == null) {
            throw new IllegalArgumentException("user and user.id cannot be null");
        }

        try {
            return sendRequest("POST", "sender", request);
        } catch (IOException e) {
            throw new NotificationApiException("Failed to send notification", e);
        }
    }

    private String sendRequest(String method, String uri, Object data) throws IOException {
        HttpRequestBase request;
        String url = BASE_URL + "/" + clientId + "/" + uri;

        switch (method) {
            case "GET":
                request = new HttpGet(url);
                break;
            case "POST":
                request = new HttpPost(url);
                break;
            case "PUT":
                request = new HttpPut(url);
                break;
            case "DELETE":
                request = new HttpDelete(url);
                break;
            case "PATCH":
                request = new HttpPatch(url);
                break;
            default:
                throw new IllegalArgumentException("Unsupported HTTP method: " + method);
        }

        request.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + authToken);
        request.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");

        if (data != null && !(request instanceof HttpGet || request instanceof HttpDelete)) {
            String jsonPayload = objectMapper.writeValueAsString(data);
            ((HttpEntityEnclosingRequestBase) request).setEntity(new StringEntity(jsonPayload, StandardCharsets.UTF_8));
        }

        HttpResponse response = httpClient.execute(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        int statusCode = response.getStatusLine().getStatusCode();

        if (statusCode < HttpStatus.SC_OK || statusCode >= HttpStatus.SC_MULTIPLE_CHOICES) {
            throw new NotificationApiException(
                "API request failed with status " + statusCode + ": " + responseBody,
                statusCode
            );
        }

        return responseBody;
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
    }
} 