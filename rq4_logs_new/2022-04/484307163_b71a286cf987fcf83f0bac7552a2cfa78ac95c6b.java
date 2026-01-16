package com.andrey.currencyconverter.repo.impl;

import com.andrey.currencyconverter.model.dto.CurrencyDto;
import com.andrey.currencyconverter.repo.CurrencyRepository;

import java.util.List;
import java.util.Properties;

public class ExternalServiceCurrencyRepositoryImpl implements CurrencyRepository<CurrencyDto> {

    private final Properties properties;
    private String apiUrl;

    public ExternalServiceCurrencyRepositoryImpl(Properties properties) {
        this.properties = properties;
        apiUrl = properties.getProperty("api-url");
    }

    @Override
    public CurrencyDto getByCurrencyCode(String name) {
        return null;
    }

    @Override
    public List<CurrencyDto> getAll() {
        return null;
    }
}