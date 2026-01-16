package com.kodilla.testing.shape;

import java.util.Objects;

public class Circle implements Shape{
    private static final String NAME = "Circle";
    private double radius;

    public Circle(double radius) {
        this.radius = radius;
    }

    private double field() {
        return Math.PI * Math.pow(radius, 2);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Circle circle = (Circle) o;
        return Double.compare(circle.radius, radius) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(radius);
    }

    @Override
    public String toString() {
        return String.format("%s(%f)", NAME, radius);
    }

    public String getShapeName() {
        return NAME;
    }

    public double getField() {
        return field();
    }
}