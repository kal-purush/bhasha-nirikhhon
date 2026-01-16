package com.it.academy.mortgage.calculator.services;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class EuriborFetcher {

    private static final String BASE_URL = "https://api.api-ninjas.com/";
    private static final String API_KEY = "444WrePC8ipo9qXgZYLzdg==0pMFBCinmqJLNGZb";

    public double fetchEuribor() throws IOException {
        String apiKey = "444WrePC8ipo9qXgZYLzdg==0pMFBCinmqJLNGZb";
        String euriborName = "Euribor - 12 month";
        String encodedEuriborName = URLEncoder.encode(euriborName, StandardCharsets.UTF_8);
        String encodedApiKey = URLEncoder.encode(apiKey, StandardCharsets.UTF_8);
        URL url = new URL("https://api.api-ninjas.com/v1/interestrate?name=" + encodedEuriborName);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestProperty("accept", "application/json");
        connection.setRequestProperty("X-Api-Key", apiKey);
        connection.setRequestProperty("accept", "application/json");
        InputStream responseStream = connection.getInputStream();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(responseStream);
        JsonNode nonCentralBankRates = root.path("non_central_bank_rates");
        return Double.parseDouble(nonCentralBankRates.get(0).path("rate_pct").asText());
    }
}