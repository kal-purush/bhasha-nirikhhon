package com.tei.tennis.point.gatewayservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tei.tennis.point.gatewayservice.model.Game;
import com.tei.tennis.point.gatewayservice.presentation.GameResult;
import com.tei.tennis.point.gatewayservice.presentation.ToResult;
import lombok.extern.slf4j.Slf4j;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
public class Command {

    Map<String, String> getReport(HttpServletRequest servletRequest, String url) {
        HttpRequest request = prepareGetRequest(servletRequest, url);
        HttpResponse<String> response = null;
        try {
            log.info("GATEWAY GET get report: " + request.uri());
            response = HttpClient.newHttpClient().send(request, BodyHandlers.ofString());
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        if (response != null) {
            return Map.of("link", response.body());
        }
        return null;
    }

    List<GameResult> getByUser(HttpServletRequest servletRequest, String url) {
        HttpRequest request = prepareGetRequest(servletRequest, url);
        HttpResponse<String> response = null;
        try {
            log.info("GATEWAY GET game: " + request.uri());
            response = HttpClient.newHttpClient().send(request, BodyHandlers.ofString());
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        if (response != null) {
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                return Arrays.stream(objectMapper.readValue(response.body(), Game[].class)).map(ToResult::execute).toList();
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        return null;
    }
    private static HttpRequest prepareGetRequest(HttpServletRequest servletRequest, String url) {
        String token = servletRequest.getHeader("Authorization");
        return HttpRequest.newBuilder().uri(URI.create(url))
            .GET()
            .header("Authorization", token)
            .header("Content-type", "application/json")
            .build();
    }

    private static HttpRequest preparePostRequest(HttpServletRequest servletRequest, String url, Object body) {
        String token = servletRequest.getHeader("Authorization");
        return HttpRequest.newBuilder().uri(URI.create(url))
            .POST(body == null ? BodyPublishers.noBody() : BodyPublishers.ofString(Objects.requireNonNull(getValue(body))))
            .header("Authorization", token)
            .header("Content-type", "application/json")
            .build();
    }

    private static String getValue(Object body) {
        try {
            return new ObjectMapper().writeValueAsString(body);
        } catch (Exception e) {
            throw new IllegalStateException("Wrong request body!");
        }
    }

}