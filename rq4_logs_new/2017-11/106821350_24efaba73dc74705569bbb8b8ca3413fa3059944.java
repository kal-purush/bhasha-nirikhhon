import javafx.scene.shape.Circle;

import javax.swing.*;
import java.awt.*;
import java.awt.geom.*;

public class MyShape {
    private Point xy1;
    private Point xy2;
    private int height;
    private int width;

    private String sketch;
    private Color color;

    public MyShape() {
    }

    public MyShape(Point xy1, Point xy2, String sketch, Object color) {
        this.xy1 = xy1;
        this.xy2 = xy2;
        height = (int) Math.abs(xy1.getY() - xy2.getY());
        width = (int) Math.abs(xy1.getX() - xy2.getX());
        this.sketch = sketch;
        this.color = (Color) color;
    }

    protected Shape figureSketch() {
        if (sketch == "Kwadrat") {
            return new Rectangle2D.Double(xy1.getX(), xy1.getY(), width, height);
        } else if (sketch == "Kolo") {
            return new Ellipse2D.Double(xy1.getX(), xy1.getY(), width, height);
        }
        return new Line2D.Double(xy1,xy2);
    }
}