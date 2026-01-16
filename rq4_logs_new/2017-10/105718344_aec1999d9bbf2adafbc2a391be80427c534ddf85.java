package line;

import geometry.Vertex3D;
import windowing.drawable.Drawable;

/**
 * Created by Andrew on 2017-10-03.
 */
public class DDALineRenderer implements LineRenderer{
    private static final int HORIZONTAL = 1;

    /**
     * implement DDA algorithm to calculate the points
     * @param p1
     * @param p2
     * @param panel
     */
    @Override
    public void drawLine(Vertex3D p1, Vertex3D p2, Drawable panel) {
        double slope = calculateSlope(p1, p2);
        double finalValue = calculateAbsValue(slope);

        double intercept = p2.getIntY() - slope * p2.getIntX();

        if(finalValue <= HORIZONTAL){
            for (int x = p1.getIntX(); x <= p2.getIntX(); x++){
                int y = calculateY(slope, x, intercept);
                panel.setPixel(x, y, p1.getZ(), p1.getColor().asARGB());
            }
        }
        else{
            for (int y = p1.getIntY(); y <= p2.getIntY(); y++){
                int x = calculateX(slope, y, intercept);
                panel.setPixel(x, y, p1.getZ(), p1.getColor().asARGB());
            }
        }
    }

    /**
     * singleton instance
     * @return
     */
    public static LineRenderer make(){
        return new AnyOctantLineRenderer(new DDALineRenderer());
    }

    private double calculateSlope(Vertex3D p1, Vertex3D p2){
        double deltaX = p2.getX() - p1.getX();
        double deltaY = p2.getY() - p1.getY();
        return deltaY/deltaX;
    }

    private double calculateAbsValue(double slope){
        return Math.abs(slope);
    }

    private int calculateY(double slope, int x, double intercept){
        return (int)(slope * x + intercept);
    }

    private int calculateX(double slope, int y, double intercept){
        return (int)((y - intercept)/slope);
    }
}