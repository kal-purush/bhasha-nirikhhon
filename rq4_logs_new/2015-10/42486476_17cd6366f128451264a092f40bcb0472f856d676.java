package com.musings.fufufanatic.coolconverterapp;

/**
 * Created by fufufanatic on 10/11/2015.
 */
public class TemperatureConversions {
    public static double celsiusToFahrenheit(double inputValue){
        double result = (inputValue * (9/5)) + 32;
        return result;
    }

    public static double celsiusToKelvin(double inputValue){
        double result = inputValue + 273.15;
        return result;
    }

    public static double fahrenheitToCelsius(double inputValue){
        double result = (inputValue - 32) * (5/9);
        return result;
    }


    public static double fahrenheitToKelvin(double inputValue){
        double result = (inputValue - 32) * (5/9) + 273.15;
        return result;
    }

    public static double kelvinToCelsius(double inputValue){
        double result = inputValue - 273.15;
        return result;
    }

    public static double kelvinToFahrenheit(double inputValue){
        double result = (inputValue - 273.15) * (9/5) + 32;
        return result;
    }

}