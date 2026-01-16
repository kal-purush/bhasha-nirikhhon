package hu.oe.nik.szfmv.automatedcar.sensors;

import java.awt.*;
import static java.lang.Math.cos;
import static java.lang.Math.sin;
import static java.lang.Math.toRadians;
import static java.lang.Math.toDegrees;

import static java.lang.Math.round;
public class Triangle {

    public static Point[] createTrianglePoints(Point sensorPos, double range, double fov, double rotation) {
        Point[] trianglePoints = new Point[3];

        double preCalAngle = fov / 2.0;
        double triangleRange = range / cos(toRadians(preCalAngle));
        Point sideA = polarPointCalc(triangleRange,  + preCalAngle + toDegrees(rotation));
        sideA.x += sensorPos.x;
        sideA.y += sensorPos.y;
        Point sideB = polarPointCalc(triangleRange,  -preCalAngle + toDegrees(rotation));
        sideB.x += sensorPos.x;
        sideB.y += sensorPos.y;

        trianglePoints[0] = sensorPos;
        trianglePoints[1] = sideA;
        trianglePoints[2] = sideB;
        return trianglePoints;




    }

    private static Point polarPointCalc(Double r, Double fov) {
        Double x = (r * cos(toRadians(fov)));
        Double y = (r * sin(toRadians(fov)));
        return new Point((int)round(x), (int)round(y));
    }
}