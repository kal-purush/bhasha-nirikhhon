package com.example;


import com.example.domain.Weather;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class JsonRequest {
    public static void main(String[] args) {
        try {
            for (int i = 0; i < 6; i++) {
                JsonRequest.call_me();
                TimeUnit.MILLISECONDS.sleep(3000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void call_me() throws Exception {
        String url = "http://api.openweathermap.org/data/2.5/weather?q=London,uk&APPID=e004dc0529d30b9531cf5dc80d31cb97";
        URL obj = new URL(url);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();
        BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuilder response = new StringBuilder();
        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();

        JSONObject myResponse = new JSONObject(response.toString());

        String name = myResponse.getString("name");
        double temp = myResponse.getJSONObject("main").getDouble("temp");
        int humidity = myResponse.getJSONObject("main").getInt("humidity");

        Weather weather = new Weather(name, temp, humidity, new Date(System.currentTimeMillis()));

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.writeValue(new File("D:\\Tools\\input/weather.json"), weather);
    }
}