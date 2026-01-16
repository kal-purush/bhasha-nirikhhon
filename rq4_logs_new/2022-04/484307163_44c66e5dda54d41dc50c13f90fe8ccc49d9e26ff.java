package com.andrey.currencyconverter.controllers.impl;

import com.andrey.currencyconverter.model.CurrencyRate;
import com.andrey.currencyconverter.model.CurrencyType;
import com.andrey.currencyconverter.model.dto.CurrencyDto;
import com.andrey.currencyconverter.repo.CurrencyRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

class ControllerImplTest {

    @Mock
    private CurrencyRepository repository;
    private AutoCloseable autoCloseable;
    private ControllerImpl underTest;

    @BeforeEach
    void setUp() {
        autoCloseable = MockitoAnnotations.openMocks(this);
        underTest = new ControllerImpl(repository);
    }

    @AfterEach
    void tearDown() throws Exception {
        autoCloseable.close();
    }

    @Test
    void canConvertCurrency() {
        //given
        String rubles = "150";
        String targetCurrencyCode = "USD";
        double convertedCurrencyBalance = 150.0 / (0.76671730591219 * 100);
        when(repository.getByCurrencyCode(targetCurrencyCode)).thenReturn(
                CurrencyRate.builder()
                        .code(CurrencyType.USD)
                        .rate(0.76671730591219)
                        .build());

        //when
        CurrencyDto currencyDto = underTest.convertCurrency(rubles, targetCurrencyCode);

        //then
        assertEquals(currencyDto.getInitialAmountOfRubles(), 150.0);
        assertEquals(currencyDto.getAmountOfTargetCurrency(), convertedCurrencyBalance);
    }
}