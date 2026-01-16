package com.example.trippe.model;

public class Address {

    private String address;
    private String city;
    private String state;
    private int zipCode;
    private String country;

    public Address(String address, String city, String state, int zipCode, String country) {
        this.address = address;
        this.city = city;
        this.state = state;
        this.zipCode = zipCode;
        this.country = country;
    }

    public Address(String city, String state, int zipCode) {
        this.city = city;
        this.state = state;
        this.zipCode = zipCode;
    }

    public Address(String city, String country) {
        this.city = city;
        this.country = country;
    }


    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public int getZipcode() {
        return zipCode;
    }

    public void setZipcode(int zipcode) {
        this.zipCode = zipcode;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }
}