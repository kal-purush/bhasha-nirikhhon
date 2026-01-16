package com.hyep.movietracker.models;

public class PersonalSpace {

    private String title;
    private int number;
    private int icon;
    private String color;

    public PersonalSpace(String title, int number, int icon, String color) {
        this.title = title;
        this.number = number;
        this.icon = icon;
        this.color = color;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    public int getIcon() {
        return icon;
    }

    public void setIcon(int icon) {
        this.icon = icon;
    }

    public String getColor() {
        return color;
    }

    public void setColor(String color) {
        this.color = color;
    }
}