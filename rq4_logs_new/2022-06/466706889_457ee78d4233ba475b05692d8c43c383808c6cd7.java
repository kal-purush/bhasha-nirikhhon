package com.example.studiograficzne;

public class Card {
    private String id;
    private int level;
    private int price;
    private int resources;

    public Card(String id, int level, int price, int resources) {
        this.id = id;
        this.level = level;
        this.price = price;
        this.resources = resources;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public int getPrice() {
        return price;
    }

    public void setPrice(int price) {
        this.price = price;
    }

    public int getResources() {
        return resources;
    }

    public void setResources(int resources) {
        this.resources = resources;
    }
}