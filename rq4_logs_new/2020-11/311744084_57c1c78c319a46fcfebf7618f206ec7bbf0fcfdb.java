package com.partnersincrime.foxdarkmaster.geekeventsmobileapp.Models;

public class TicketModel {

    private final String name;
    private final String description;
    private final double price;
    private final String currency;

    public TicketModel(String name, String description, double price, String currency) {
        this.name = name;
        this.description = description;
        this.price = price;
        this.currency = currency;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public double getPrice() {
        return price;
    }

    public String getCurrency() {
        return currency;
    }

}