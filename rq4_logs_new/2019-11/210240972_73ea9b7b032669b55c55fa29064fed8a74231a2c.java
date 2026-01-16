package com.example.trippe.ui.currency;

import java.util.Date;
import java.util.Map;

public class TrippeCurrency {
    private Map<Date, Double> rates;
    private long flagResourceid;
    private String abbreviation;
    private String name;
    private String country;

    public TrippeCurrency() {

    }

    public TrippeCurrency(String abbreviation ) {
        this.abbreviation = abbreviation;
    }

    public TrippeCurrency(String abbreviation, String name) {
        this.abbreviation = abbreviation;
        this.name = name;
    }

    public void sortDates() {

    }

    public String getCurrencyName() {
        return this.name;
    }

    public String getAbbreviation() {
        return this.abbreviation;
    }

    public String getCountry() {
        return this.country;
    }

    public long getFlagResourceId(){
        return this.flagResourceid;
    }

    public void addRate(Date date, Double rate) {
        this.rates.put(date, rate);
    }

    public void setAbbreviation(String abbrev){
        this.abbreviation = abbrev;
    }
}