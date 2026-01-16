package com.example.mycalculator;

public class Calculator {

    public Double calculate(String operation, double firstDigit, double secondDigit) {
        switch (operation) {
            case "/": {
                return firstDigit / secondDigit;
            }
            case "*": {
                return firstDigit * secondDigit;
            }
            case "-": {
                return firstDigit - secondDigit;
            }
            case "+": {
                return firstDigit + secondDigit;
            }
        }
        return -1.0;
    }
}