package com.epicodus.rxjavaproject;

/**
 * Interface allowing for communication between
 */
public interface WeatherContract {
    interface Presenter {
        void getWeather(String city);
    }

    interface View {
        void showWeather(String description, float temp, float windSpeed);
    }
}