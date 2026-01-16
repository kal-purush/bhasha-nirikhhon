package com.example.planties.core.utils;

import java.util.HashMap;

public class NetworkUtil {

    public static HashMap<String, String> getAuthHeader(String token) {
        HashMap<String, String> header = new HashMap<>();
        header.put("Authorization", "Bearer " + token);

        return header;
    }

    public static HashMap<String, String> getAuthHeaderRefresh(String refreshToken) {
        HashMap<String, String> header = new HashMap<>();
        header.put("Authorization", "Bearer " + refreshToken);

        return header;
    }
}