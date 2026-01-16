package com.example.calculator;

public class Calculator {

    public int add(int a, int b) {
        return a + b;
    }

    public int division(int a, int b) {
    /*    if (b ==0 ){
            throw new ArithmeticException();
        } else {
            return a / b;
        }*/

        try{
            return a / b;
        } catch(ArithmeticException ex) {
            throw new ArithmeticException("0으로 나눌수 없습니다.");
        }
    }

    public double division(double a, double b) {
        return a / b;
    }

    public int subtract(int a, int b){
        return a-b;
    }

    public int multiply(int a, int b){
        return a*b;
    }
}