package com.example.trippe;

import android.content.Context;

public class CurrencyQuery {
    private String sourceCurrency = "";
    private String destCurrency = "";
    private float sourceAmount = 0;
    private float destAmount = 0;
    private Context context;

    public CurrencyQuery(Context context) {
        this.context = context;
    }
    public CurrencyQuery(Context context, String dest) {
        this.context = context;
        setDestCurrency(dest);
    }

    public CurrencyQuery(Context context, String dest, String source) {
        this.context = context;
        setDestCurrency(dest);
        setSourceCurrency(source);
    }

    public CurrencyQuery(Context context, String dest, float amount) {
        this.context = context;
        setDestCurrency(dest);
        setSourceAmount(amount);
    }

    public CurrencyQuery(Context context, String dest, String source, float amount) {
        this.context = context;
        setDestCurrency(dest);
        setSourceCurrency(source);
        setSourceAmount(amount);
    }

    public String getSourceCurrency() { return sourceCurrency; }
    public String getDestCurrency() { return destCurrency; }
    public float getSourceAmount() { return sourceAmount; }
    public float getDestAmount() { return destAmount; }

    public void setSourceCurrency(String source) { // set our local sourceCurrency if its a valid entry
        if (validCurrency(source) && source != this.destCurrency) {
            this.sourceCurrency = source;
        }
    }

    public void setDestCurrency(String dest) {
        if (validCurrency(dest) && dest != this.sourceCurrency) {
            this.destCurrency = dest;
        }
    }

    public void setSourceAmount(float amount) { // standard setter, ensures amount is > 0
        if (amount > 0 ) {
            this.sourceAmount = amount;
        }
    }

    private boolean validCurrency(String currency) {
        if (currency.length() != 3) {
            return false;
        }

        String currencyOptions[]; // will store the values of our resource string array in strings.xml
        currencyOptions = context.getResources().getStringArray(R.array.currency_array);

        // iterate through our currency list to see if we passed a valid currency
        for (String currencyEntry : currencyOptions) {
            if (currencyEntry == currency) {
                return true;
            }
        }
        return false;
    }
}