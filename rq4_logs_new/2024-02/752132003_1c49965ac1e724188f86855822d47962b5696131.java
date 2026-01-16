package com.web.data;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class TestDate {
    public static void main(String[] args) {
        try {
            String serviceKey = "8RQmmNMbqQKZO06m6d44ZNTJv55aWC7ld4cj5de9n14a6o3tbFOrn/F3Aa5cVQzRVlpUr2nt2J9sjnqrnD2KLA==";
            String seriesCd = "43";

            String url = "http://openapi.q-net.or.kr/api/service/rest/InquiryTestDatesNationalProfessionalQualificationSVC/getList";
            String encodedUrl = url + "?ServiceKey=" + URLEncoder.encode(serviceKey, StandardCharsets.UTF_8) +
                    "&seriesCd=" + URLEncoder.encode(seriesCd, StandardCharsets.UTF_8);

            HttpClient httpClient = HttpClient.newHttpClient();
            HttpRequest httpRequest = HttpRequest.newBuilder()
                    .uri(URI.create(encodedUrl))
                    .GET()
                    .build();

            HttpResponse<String> httpResponse = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());

            System.out.println("Response code: " + httpResponse.statusCode());
            System.out.println(httpResponse.body());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}