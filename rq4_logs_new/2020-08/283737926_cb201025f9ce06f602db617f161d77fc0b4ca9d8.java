package com.example.geekbrainsandroidweather.model;

public class HourlyForecastData {
    private String time;
    private String stateImage;
    private String temperature;

    public HourlyForecastData(String time, String stateImage, String temperature) {
        this.time = time;
        this.stateImage = stateImage;
        this.temperature = temperature;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getStateImage() {
        return stateImage;
    }

    public void setStateImage(String stateImage) {
        this.stateImage = stateImage;
    }

    public String getTemperature() {
        return temperature;
    }

    public void setTemperature(String temperature) {
        this.temperature = temperature;
    }
}