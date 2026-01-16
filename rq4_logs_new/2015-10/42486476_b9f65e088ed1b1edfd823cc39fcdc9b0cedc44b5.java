package com.musings.fufufanatic.coolconverterapp;

/**
 * Created by fufufanatic on 10/8/2015.
 */
public class WeightConversions {

    public static double poundsToKilograms(double inputValue){
        double result = inputValue / 2.2046;
        return result;
    }

    public static double poundsToOunces(double inputValue){
        double result = inputValue * 16.000;
        return result;
    }

    public static double poundsToMilligrams(double inputValue){
        double result = inputValue * 453592.37;
        return result;
    }

    public static double kilogramsToPounds(double inputValue){
        double result = inputValue * 2.205;
        return result;
    }

    public static double kilogramsToOunces(double inputValue){
        double result = inputValue * 35.274;
        return result;
    }

    public static double kilogramsToMilligrams(double inputValue){
        double result = inputValue * 1000000;
        return result;
    }

    public static double ouncesToKilograms(double inputValue){
        double result = inputValue * 0.0283495231;
        return result;
    }

    public static double ouncesToPounds(double inputValue){
        double result = inputValue / 16;
        return result;
    }

    public static double ouncesToMilligrams(double inputValue){
        double result = inputValue * 28349.5231;
        return result;
    }

    public static double milligramsToKilogram(double inputValue){
        double result = inputValue / 1000000;
        return result;
    }

    public static double milligramsToOunces(double inputValue){
        double result = inputValue / 28349.5231;
        return result;
    }

    public static double milligramsToPounds(double inputValue){
        double result = inputValue / 453592.37;
        return result;
    }
}