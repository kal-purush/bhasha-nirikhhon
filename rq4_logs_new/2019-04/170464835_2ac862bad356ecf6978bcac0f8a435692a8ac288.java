package hu.oe.nik.szfmv.automatedcar.virtualfunctionbus.packets.Camera;

import hu.oe.nik.szfmv.automatedcar.model.WorldObject;

import java.awt.*;
import java.util.ArrayList;
import java.util.List;

public class SimpleDetector {


    private static SimpleDetector detector;
    private List<WorldObject> worldObjects;

    private SimpleDetector() {

    }


    private Polygon createTriangle(Point a, Point b, Point c) {
        Polygon p = new Polygon();
        p.addPoint(a.x, a.y);
        p.addPoint(b.x, b.y);
        p.addPoint(c.x, c.y);

        return p;
    }



    private List<WorldObject> getTheObjectsWhichAreInTheTriangle(Point a, Point b, Point c) {
        Shape sensor = createTriangle(a, b, c);
        List<WorldObject> visibleObj = new ArrayList<WorldObject>();
        for (int x = 0; x < worldObjects.size(); x++) {
            WorldObject act = worldObjects.get(x);
            if (act.getShape().intersects(sensor.getBounds())) {
                visibleObj.add(act);
            }
        }
        return visibleObj;
    }

    /**
     * <<<<<<< HEAD
     *
     * @return worldobjects
     */
    public List<WorldObject> getWorldObjects() {
        return worldObjects;
    }

    /**
     * Creates a Detector and sets the Detector variable
     *
     * @return Detector responsible for sensor functionality
     */
    public static SimpleDetector getDetector() {
        if (detector == null) {

            detector = new SimpleDetector();
            return detector;

        } else {
            return detector;
        }
    }

    public void setWorldObjects(List<WorldObject> wo) {
        worldObjects = wo;
    }



    public List<WorldObject> getWorldObjects(Point a, Point b, Point c) {
        return  getTheObjectsWhichAreInTheTriangle(a, b, c);

    }


}