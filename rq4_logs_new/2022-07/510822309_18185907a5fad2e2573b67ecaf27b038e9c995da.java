package ru.sber.oop2;

public class Circle extends Figure implements Drawable{
    double radius;

    public Circle(Point points, int radius) {
        super(points, "круг");
        this.radius = radius;
    }

    @Override
    public double area() {
        return 3.14 * radius * radius;
    }

    @Override
    public double perimeter() {
        return 2 * 3.14 * radius;
    }

    @Override
    public void draw(Color color) {
        System.out.println(String.format("Нарисован круг, цвет: %s", color));
    }

    @Override
    public void draw() {
        System.out.println("Нарисован круг, цвет: BLACK");
    }
}