package com.kodilla.stream.world;

import java.math.BigDecimal;

public final class Country {
    private final String countryName;
    private final BigDecimal countryPeopleQuantity;

    public Country(final String countryName, final BigDecimal countryPeopleQuantity) {
        this.countryName = countryName;
        this.countryPeopleQuantity = countryPeopleQuantity;
    }

    public String getCountryName() {
        return countryName;
    }

    @Override
    public String toString() {
        return "Country{" +
                "countryName='" + countryName + '\'' +
                ", countryPeopleQuantity=" + countryPeopleQuantity +
                '}';
    }

    public BigDecimal getPeopleQuantity() {
        return countryPeopleQuantity;
    }
}