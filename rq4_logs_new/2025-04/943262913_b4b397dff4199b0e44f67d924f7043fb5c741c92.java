package com.sign.controller;

import jakarta.servlet.http.Cookie;
import java.util.Arrays;
import org.springframework.http.ResponseCookie;

public class CookieManager {

    public ResponseCookie provide(String name, String value) {
        return ResponseCookie.from(name)
                .value(value)
                .httpOnly(true)
                .build();
    }

    public String findByKey(Cookie[] cookies, String name) {
        return Arrays.stream(cookies)
                .filter(it -> it.getName().equals(name))
                .findFirst()
                .map(Cookie::getName)
                .orElse("");
    }
}