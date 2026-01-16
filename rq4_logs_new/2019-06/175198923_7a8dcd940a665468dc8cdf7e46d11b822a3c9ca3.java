package sw19.moring03.paint.utils;

import java.util.ArrayList;
import java.util.List;

import sw19.moring03.paint.tools.TextTool;
import sw19.moring03.paint.tools.Tools;

public class DrawingObjectManager {

    private List<Tools> objectsToPaint;
    private List<Tools> objectsToRedo;

    public DrawingObjectManager() {
        objectsToPaint = new ArrayList<>();
        objectsToRedo = new ArrayList<>();
    }

    public List<Tools> getObjectsToPaint() {
        return objectsToPaint;
    }

    public List<Tools> getObjectsToRedo() {
        return objectsToRedo;
    }

    public void addPoint(PointF point) {
        if (objectsToPaint.size() != 0) {
            objectsToPaint.get(objectsToPaint.size() - 1).addPoint(point);
        }
    }

    public void addTool(Tools tool) {
        objectsToPaint.add(tool);
    }

    public void addTextToTool(String text) {
        ((TextTool)objectsToPaint.get(objectsToPaint.size() - 1)).setText(text);
    }

    public void addFontToTool(String font) {
        ((TextTool)objectsToPaint.get(objectsToPaint.size() - 1)).setFont(font);
    }

    public void removeLastElementFromPaintList() {
        if (objectsToPaint != null && !objectsToPaint.isEmpty()) {
            objectsToRedo.add(objectsToPaint.get(objectsToPaint.size() - 1));
            objectsToPaint.remove(objectsToPaint.size() - 1);
        }
    }

    public void addLastRemovedElementToPaintList() {
        if (objectsToRedo != null && !objectsToRedo.isEmpty()) {
            objectsToPaint.add(objectsToRedo.get(objectsToRedo.size() - 1));
            objectsToRedo.remove(objectsToRedo.size() - 1);
        }
    }

    public boolean isUndoPossible() {
        return !objectsToPaint.isEmpty();
    }

    public boolean isRedoPossible() {
        return !objectsToRedo.isEmpty();
    }

}