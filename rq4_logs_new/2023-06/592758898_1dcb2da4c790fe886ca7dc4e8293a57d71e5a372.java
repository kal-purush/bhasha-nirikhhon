package org.ua.javaPro.berezhnoy.bank.currencyConverterAPI;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Currency;

@RestController
@RequestMapping("/currency")
@RequiredArgsConstructor
public class ControllerCurrency {
    private final CurrencyConverter currencyConvertor;

    @GetMapping
    public Object convert(
            @RequestParam("from") Currency from,
            @RequestParam("to") Currency to,
            @RequestParam("amount") Integer amount
    ) {

        return currencyConvertor.convert(from, to, amount);
    }
}