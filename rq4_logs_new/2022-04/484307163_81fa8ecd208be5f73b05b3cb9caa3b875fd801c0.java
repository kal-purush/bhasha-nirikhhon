package com.andrey.currencyconverter.repo.impl;

import com.andrey.currencyconverter.model.CurrencyRate;
import com.andrey.currencyconverter.model.CurrencyType;
import com.andrey.currencyconverter.repo.CurrencyRepository;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;

class JsonFileCurrencyRepositoryImplTest {

    private CurrencyRate getRate() {
        CurrencyRate currencyRate = new CurrencyRate();
        currencyRate.setRate(1L);
        currencyRate.setCode(CurrencyType.GBP);
        return currencyRate;
    }

    @Test
    void shouldReturnCurrencyRateByCurrencyCode() {
        //given
        CurrencyRepository<CurrencyRate> mockRepository = Mockito.mock(JsonFileCurrencyRepositoryImpl.class);
        CurrencyRate rate = getRate();
        String currencyCode = "GBP";
        //when
        Mockito.when(mockRepository
                        .getByCurrencyCode(currencyCode))
                .thenReturn(rate);
        //then
        assertEquals(mockRepository.getByCurrencyCode(currencyCode).getCode(),CurrencyType.GBP);
    }

    @Test
    void shouldThrowNoSuchElementExceptionWhenCurrencyCodeNotValid() {
        //given
        CurrencyRepository<CurrencyRate> mockRepository = Mockito.mock(JsonFileCurrencyRepositoryImpl.class);
        //when
        Mockito.when(mockRepository
                        .getByCurrencyCode(any(String.class)))
                .thenThrow(NoSuchElementException.class);
        //then
        assertThrows(NoSuchElementException.class,
                () -> mockRepository.getByCurrencyCode("WRONG_CODE").getCode());
    }

    @Test
    void shouldReturnList() {
        //given
        CurrencyRepository<CurrencyRate> mockRepository = Mockito.mock(JsonFileCurrencyRepositoryImpl.class);
        List<CurrencyRate> list = new ArrayList<>();
        list.add(getRate());
        list.add(getRate());
        //when
        Mockito.when(mockRepository.getAll()).thenReturn(list);
        //then
        assertEquals(2, mockRepository.getAll().size());
    }
}