package com.github.user;

/**
 * Created by MiluchM on 2015-01-29.
 */
public class Address {
    //TODO: homework -> utworzyc klase Address reprezentujaca adres pocztowy (ulica, numer domu, numer mieszkania,miasto)

    private String street;
    private String houseNumber;
    private String apartmentNumber;
    private String city;


    public Address(String street, String houseNumber, String apartmentNumber, String city) {
        this.street = street;
        this.houseNumber = houseNumber;
        this.apartmentNumber = apartmentNumber;
        this.city = city;
    }

    public String getStreet() {
        return street;
    }

    public void setStreet(String street) {
        this.street = street;
    }

    public String getHouseNumber() {
        return houseNumber;
    }

    public void setHouseNumber(String houseNumber) {
        this.houseNumber = houseNumber;
    }

    public String getApartmentNumber() {
        return apartmentNumber;
    }

    public void setApartmentNumber(String apartmentNumber) {
        this.apartmentNumber = apartmentNumber;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }
}