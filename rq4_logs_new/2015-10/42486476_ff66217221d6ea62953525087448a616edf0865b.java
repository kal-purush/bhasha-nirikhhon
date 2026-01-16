package com.musings.fufufanatic.coolconverterapp;

/**
 * Created by fufufanatic on 10/1/2015.
 */
public class DistanceConversions {

    public static double inchesToFeet(double inputValue){
        double result = inputValue / 12;
        return result;
    }

    public static double inchesToYards(double inputValue){
        double result = inputValue / 36;
        return result;
    }

    public static double inchesToCentimeters(double inputValue){
        double result = inputValue * 2.54;
        return result;
    }

    public static double feetToInches(double inputValue){
        double result = inputValue * 12;
        return result;
    }

    public static double feetToYards(double inputValue){
        double result = inputValue / 3;
        return result;
    }

    public static double feetToCentimeters(double inputValue){
        double result = inputValue * 30.48;
        return result;
    }

    public static double yardsToInches(double inputValue){
        double result = inputValue * 36;
        return result;
    }

    public static double yardsToFeet(double inputValue){
        double result = inputValue * 3;
        return result;
    }

    public static double yardsToCentimeters(double inputValue){
        double result = inputValue * 91.4;
        return result;
    }

    public static double centimetersToInches(double inputValue){
        double result = inputValue / 2.54;
        return result;
    }

    public static double centimetersToFeet(double inputValue){
        double result = inputValue / 30.48;
        return result;
    }

    public static double centimetersToYards(double inputValue){
        double result = inputValue / 91.4;
        return result;
    }
}