package ru.sber.unitTest.main;

public class Figure extends ArithmeticOperation {

    private final static double PI = 3.14;

    public double perimeterSquare(int a) {
        return multiplication(4,a);
    }

    public double areaSquare(int a) {
        return involution(a, 2);
    }

    public double perimeterRectangle(int a, int b) {
        return multiplication(summation(a, b), 2);
    }

    public double areaRectangle(int a, int b) {
        return multiplication(a, b);
    }

    public double perimeterCircle(double r) {
        return multiplication(multiplication(2, PI), r);
    }

    public double areaCircle(double r) {
        return multiplication(PI, involution(r, 2));
    }

}