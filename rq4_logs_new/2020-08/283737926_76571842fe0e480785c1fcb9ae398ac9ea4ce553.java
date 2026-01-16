package com.example.geekbrainsandroidweather.model;

public class ForecastDayData {
    private String day;
    private String image;
    private String state;
    private String temperature;

    public String getDay() {
        return day;
    }

    public ForecastDayData withDay(String day) {
        this.day = day;
        return this;
    }

    public String getImage() {
        return image;
    }

    public ForecastDayData withImage(String image) {
        this.image = image;
        return this;
    }

    public String getTemperature() {
        return temperature;
    }

    public ForecastDayData withTemperature(String temperature) {
        this.temperature = temperature;
        return this;
    }

    public String getState() {
        return state;
    }

    public ForecastDayData withState(String state) {
        this.state = state;
        return this;
    }
}