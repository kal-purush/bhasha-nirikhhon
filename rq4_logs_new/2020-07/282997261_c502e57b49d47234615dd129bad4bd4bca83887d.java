package pl.recruitment.rtg;

import javafx.scene.shape.Circle;

public class MyCircle extends Circle {

    private Double x;
    private Double y;

    public MyCircle(double radius, Double x, Double y) {
        super(radius);
        this.x = x;
        this.y = y;
    }

    public Double getX() {
        return x;
    }

    public void setX(Double x) {
        this.x = x;
    }

    public Double getY() {
        return y;
    }

    public void setY(Double y) {
        this.y = y;
    }
}