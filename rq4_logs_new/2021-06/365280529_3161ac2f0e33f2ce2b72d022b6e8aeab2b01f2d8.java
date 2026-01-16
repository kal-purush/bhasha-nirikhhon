package com.task.exchangerates.controller;

import com.task.exchangerates.service.ExRatesService;
import com.task.exchangerates.util.converter.CurrencyToCurrencyDtoConverter;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@WebMvcTest(ExRatesController.class)
class ExRatesControllerTest {

    private static final String URI = "/api/v1/exchange-rates";

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private ExRatesService service;

    @MockBean
    private CurrencyToCurrencyDtoConverter converter;

    @Test
    void shouldReturnedRates() throws Exception {
        MvcResult result = mockMvc.perform(MockMvcRequestBuilders.get(URI)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk()).andReturn();

    }

    @Test
    void shouldExchangedFromOneToSameRate() {
    }

    @Test
    void shouldExchangedFromOneToAnotherRates() {
    }

    @Test
    void shouldReturnedRatesList() {
    }
}