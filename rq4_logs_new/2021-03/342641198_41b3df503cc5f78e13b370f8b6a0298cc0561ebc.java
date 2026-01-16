package com.banking.bankservice.service.impl;

import com.banking.bankservice.model.Currency;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class Converter {
    @Value(value = "$(service.url)")
    private String apiUrl;
    private final HttpClientService httpClientService;

    @Autowired
    public Converter(HttpClientService httpClientService)cr {
        this.httpClientService = httpClientService;
    }

    public Double convert(Currency from, Currency to, Double amount) {
        String url = String.format(apiUrl + "?from=%s&to=%s&amount=%s", from, to, amount);
        return httpClientService.exchange(url);
    }
}