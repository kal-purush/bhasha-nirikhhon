package com.reservibe.domain.entity.restaurant;

public class Address {

    private String street;
    private Integer number;
    private String neighborhood;
    private String city;
    private String state;
    private String country;
    private String zipCode;

    public Address(String street, Integer number, String neighborhood, String city, String state, String country, String zipCode) {
        this.street = street;
        this.number = number;
        this.neighborhood = neighborhood;
        this.city = city;
        this.state = state;
        this.country = country;
        this.zipCode = zipCode;
    }

    public String getStreet() {
        return street;
    }

    public Integer getNumber() {
        return number;
    }

    public String getNeighborhood() {
        return neighborhood;
    }

    public String getCity() {
        return city;
    }

    public String getState() {
        return state;
    }

    public String getCountry() {
        return country;
    }

    public String getZipCode() {
        return zipCode;
    }
}