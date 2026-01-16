package com.ineedyourcode.calculator.presenter;

public class Calculator implements Calculationable {
    @Override
    public double arithmeticOperation(double argOne, double argTwo, ArithmeticOperation operation) {
        switch (operation) {
            case PLUS:
                return argOne + argTwo;
            case MINUS:
                return argOne - argTwo;
            case MULTIPLY:
                return argOne * argTwo;
            case DIVISION:
                return argOne / argTwo;
        }
        return 0;
    }
}
