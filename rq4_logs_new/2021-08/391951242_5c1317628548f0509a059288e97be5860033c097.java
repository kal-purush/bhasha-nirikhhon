package ru.geekbrains.lesson1.les1x3;

public class Circle extends Shape<Double> implements Drawable {
    @Override
    public void draw() {
        System.out.println("This is a Circle");
    }

    @Override
    public Double square(Double param) {
        return Math.PI * param;
    }
}