package com.es.phoneshop.model.product;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Currency;

public class PriceRecord {
    private Calendar data;
    private BigDecimal price;
    private Currency currency;

    public PriceRecord(Calendar data, BigDecimal price, Currency currency) {
        this.data = data;
        this.price = price;
        this.currency = currency;
    }

    public Calendar getData() {
        return data;
    }

    public void setData(Calendar data) {
        this.data = data;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }

    public Currency getCurrency() {
        return currency;
    }

    public void setCurrency(Currency currency) {
        this.currency = currency;
    }
}