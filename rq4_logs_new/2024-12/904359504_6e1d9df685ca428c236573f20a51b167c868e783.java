package com.example.pizza.config;

import com.paypal.base.rest.APIContext;
import com.paypal.base.rest.PayPalRESTException;
import com.paypal.base.rest.OAuthTokenCredential;

import java.util.HashMap;
import java.util.Map;

import org.springframework.context.annotation.Configuration;

import org.springframework.context.annotation.Bean;

@Configuration
public class PayPalConfig {

    private static final String CLIENT_ID = "AaOR8kTaERZ4i0vSKSNqtzDyYLIJLDLylG3pG-2hTOknfxLklsaC3FDZbg_UlCVWSGFbk2emVjQhOyRU";
    private static final String CLIENT_SECRET = "EM69382ECPZo4lr168RUmd1SfANTJAf0WOFlnRYqU4ye8uLgs-RWVJU0XQqazk3yHr-dhQXk7sO8iFZl";
    private static final String MODE = "sandbox"; // Hoặc "live" cho môi trường sản xuất

    @Bean
    APIContext apiContext() {
        // Cấu hình các tham số SDK
        Map<String, String> sdkConfig = new HashMap<>();
        sdkConfig.put("mode", MODE);

        return new APIContext(CLIENT_ID, CLIENT_SECRET, MODE, sdkConfig);
    }
}