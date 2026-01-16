package com.vladimirsimek.csgo;

public class Diamond {
    //"","carat","cut","color","clarity","depth","table","price","x","y","z"
    int number;
    double carat;
    String cut;
    String color;
    String clarity;
    int price;
    double x;
    double y;

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    public double getCarat() {
        return carat;
    }

    public void setCarat(double carat) {
        this.carat = carat;
    }

    public String getCut() {
        return cut;
    }

    public void setCut(String cut) {
        this.cut = cut;
    }

    public String getColor() {
        return color;
    }

    public void setColor(String color) {
        this.color = color;
    }

    public String getClarity() {
        return clarity;
    }

    public void setClarity(String clarity) {
        this.clarity = clarity;
    }

    public int getPrice() {
        return price;
    }

    public void setPrice(int price) {
        this.price = price;
    }

    public double getX() {
        return x;
    }

    public void setX(double x) {
        this.x = x;
    }

    public double getY() {
        return y;
    }

    public void setY(double y) {
        this.y = y;
    }

    public Diamond(int number, double carat, String cut, String color, String clarity, int price, double x, double y) {
        this.number = number;
        this.carat = carat;
        this.cut = cut;
        this.color = color;
        this.clarity = clarity;
        this.price = price;
        this.x = x;
        this.y = y;
    }
}