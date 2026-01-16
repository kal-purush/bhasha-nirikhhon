package com.company;


public class AddressBuilder {
    public static String city;
    private static String state;
    private static String address;

    private String getCity() {
        return this.city;
    }

    private String setCity(String value) {
        return this.city = value;
    }

    public AddressBuilder(String addressString) {
    }

}