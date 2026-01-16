package com.jkl4o4.cocktail;

import com.google.gson.Gson;

import java.util.Map;

public class Cocktail {
    static {
        System.loadLibrary("go");
    }
    private static native String buildURL(String domain, String map);

    public static String generateURL(String domain, Map<String, String> map) {
        Gson gson = new Gson();
        return buildURL(domain, gson.toJson(map));
    }
}