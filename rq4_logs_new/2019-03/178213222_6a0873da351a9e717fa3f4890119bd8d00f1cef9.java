/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.example.model;

/**
 *
 * @author 2110461
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class Weather {
    private static final String USER_AGENT = "Mozilla/5.0";
    
    
    /**
     * Metodo consulta el clima en una ciudad
     * @param city a consultar
     */
    public static String getCityWeather(String city) throws IOException {
        return HttpRequert("http://api.openweathermap.org/data/2.5/weather?q="+city+"&appid=154a4767fcec3a62f99c00a41b59ab1b");

    }
    
     /**
     * Hace una consulta a una url
     * @param request peticion a hacer
     * @return el resultado de la peticion
     * @throws IOException
     */
    private static String HttpRequert(String request) throws IOException{
        String GET_URL = request;
        String value;
        URL obj = new URL(GET_URL);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();
        con.setRequestMethod("GET");
        con.setRequestProperty("User-Agent", USER_AGENT);

        //The following invocation perform the connection implicitly before getting the code
        int responseCode = con.getResponseCode();
        System.out.println("GET Response Code :: " + responseCode);

        if (responseCode == HttpURLConnection.HTTP_OK) { // success
            BufferedReader in = new BufferedReader(new InputStreamReader(
                    con.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();
            value=response.toString();
            // print result
            System.out.println(response.toString());
        } else {
            System.out.println("GET request not worked");
            value="Error";
        }
        return value;

    }
}