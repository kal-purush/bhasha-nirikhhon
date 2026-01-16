package O5_practice.exercise2_inheritance;

public class Circle {
    private double radius;
    private String color;
    public Circle() {
        this.radius = 1.0;
        this.color = "red";
    }
    public Circle(double radius) {
        this.radius = radius;
        this.color = "red";
    }

    public double getRadius () {
        return this.radius;
    }

    public double getArea () {
        return this.radius * this.radius * Math.PI;
    }

    public String toString() {
        return "Circle: radius=" + this.radius + ", color=" + this.color;
    }

}