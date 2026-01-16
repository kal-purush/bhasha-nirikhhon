package com.company.model;

public class Cargo {
    private String type;
    private double weight;
    private double volume;
    private int quantity;

    public Cargo() {
    }

    public Cargo(String type, double weight, double volume, int quantity) {
        this.type = type;
        this.weight = weight;
        this.volume = volume;
        this.quantity = quantity;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public double getVolume() {
        return volume;
    }

    public void setVolume(double volume) {
        this.volume = volume;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }
}