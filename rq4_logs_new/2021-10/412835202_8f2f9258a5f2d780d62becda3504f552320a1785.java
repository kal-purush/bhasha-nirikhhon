package java17;

public class PatternMatching {

    // Pattern matching for instanceof
    public void featurePreview(Object obj) {
        if (obj instanceof String str)
            System.out.println(str.toUpperCase());

    }

    // Pattern matching for switch
    public double getArea(Shape shape) {
        return switch (shape) {
            case Square square -> square.side * square.side;
            case Rectangle rectangle -> rectangle.width * rectangle.height;
            case Circle circle -> Math.PI * circle.radius * circle.radius;
            case null, default -> 0;
        };
    }


    static class Shape {
    }

    static class Square extends Shape {
        public int side;
    }

    static class Rectangle extends Shape {
        public int width;
        public int height;
    }

    static class Circle extends Shape {
        public int radius;
    }

}