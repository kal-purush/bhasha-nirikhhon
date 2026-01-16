package com.web.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

@RestController
public class CareerTestController {

    @Value("${career.api.key}")
    private String apiKey;

    @PostMapping("/submit-career-test")
    public ResponseEntity<String> submitCareerTest(@RequestBody String requestBody) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("apikey", apiKey); // API 키 설정

        HttpEntity<String> requestEntity = new HttpEntity<>(requestBody, headers);

        RestTemplate restTemplate = new RestTemplate();
        ResponseEntity<String> responseEntity = restTemplate.exchange(
                "http://www.career.go.kr/inspct/openapi/test/report",
                HttpMethod.POST,
                requestEntity,
                String.class);

        return ResponseEntity.status(responseEntity.getStatusCode()).body(responseEntity.getBody());
    }
}