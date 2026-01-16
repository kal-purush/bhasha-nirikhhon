package line.helper;

import geometry.Point3DH;
import geometry.Vertex3D;

/**
 * Created by Administrator on 10/7/2017.
 */
public class Supplier {
    public static int calculateY(double slope, int x, double intercept){
        return (int)(slope * x + intercept);
    }

    public static int calculateX(double slope, int y, double intercept){
        return (int)((y - intercept)/slope);
    }

    /**
     * This function swap x and y.
     * @param point
     * @return Vertex3D point
     */
    public static Vertex3D swap(Vertex3D point){
        Point3DH temp = new Point3DH(point.getY(), point.getX(), point.getZ(), point.getPoint3D().getW());
        return point.replacePoint(temp);
    }

    public static double calculateSlope(Vertex3D p1, Vertex3D p2){
        double deltaX = calculateDeltaX(p1, p2);
        double deltaY = calculateDeltaY(p1, p2);
        return deltaY/deltaX;
    }

    public static double calculateDeltaX(Vertex3D p1, Vertex3D p2){
        return p2.getX() - p1.getX();
    }

    public static double calculateDeltaY(Vertex3D p1, Vertex3D p2){
        return p2.getY() - p1.getY();
    }

    public static double calculateAbsValue(double slope){
        return Math.abs(slope);
    }
}