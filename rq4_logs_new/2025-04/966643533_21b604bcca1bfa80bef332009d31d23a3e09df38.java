package org.spiceboys.Travel.Diary.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class EndpointController {
    @GetMapping("/")
    public Map<String, String> welcome() {
        Map<String, String> response = new HashMap<>();
        response.put("message", "Welcome to the Travel Diary API!");
        response.put("status", "OK");
        response.put("version", "1.0");
        return response;
    }

    @GetMapping("/api")
    public ResponseEntity<String> getApiDocs() throws IOException {
        InputStream endpointsManual = getClass().getResourceAsStream("/endpoints.json");
        if (endpointsManual == null) {
            return ResponseEntity
                    .status(HttpStatus.NOT_FOUND)
                    .body("{\"error\": \"API documentation not found.\"}");
        }
        String jsonFormatedEndpointsManual = new String(endpointsManual.readAllBytes(), StandardCharsets.UTF_8);
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .body(jsonFormatedEndpointsManual);
    }
}