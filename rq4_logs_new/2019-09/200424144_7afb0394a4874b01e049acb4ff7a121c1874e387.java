package com.kodilla.testing.shape;

import java.util.ArrayList;
import java.util.List;

public class ShapeCollector {
    private ArrayList<Shape> shapes = new ArrayList<>();

    public void addFigure(Shape theShape) {
        shapes.add(theShape);
    }

    public boolean removeFigure(Shape theShape) {
        boolean result = false;
        if(shapes.contains(theShape)) {
            shapes.remove(theShape);
            result = true;
        }
        return result;
    }

    public Shape getFigure(int n) {
        return shapes.get(n);
    }

    public List<Shape> showFigures() {
        return shapes;
    }
}