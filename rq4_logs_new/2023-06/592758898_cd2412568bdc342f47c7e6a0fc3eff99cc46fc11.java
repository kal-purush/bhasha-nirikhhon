package org.ua.javaPro.berezhnoy.bank;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ua.javaPro.berezhnoy.bank.currencyConverterAPI.CurrencyConverter;

import java.util.Currency;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class PersonOperationsService {
    private final CurrencyConverter currencyConverter;
    private static final Logger logger = LoggerFactory.getLogger(PersonOperationsService.class);

    public PersonOperationsService(CurrencyConverter currencyConverter) {
        this.currencyConverter = currencyConverter;
    }

    public CompletableFuture<Double> convert(Currency fromCurrency, Currency toCurrency, double amount) {
        logger.info("Converting {} {} to {}", amount, fromCurrency, toCurrency);
        return CompletableFuture.supplyAsync(() ->
                currencyConverter.convert(fromCurrency, toCurrency, amount));
    }
}