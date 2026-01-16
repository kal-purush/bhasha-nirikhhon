package line;

import geometry.Vertex3D;
import line.helper.Supplier;
import windowing.drawable.Drawable;
import windowing.graphics.Color;

import static java.lang.Math.round;

/**
 * Created by Administrator on 10/3/2017.
 */
public class WuLineRenderer implements LineRenderer {
    private static final int MIDANGLE = 1;


    @Override
    public void drawLine(Vertex3D p1, Vertex3D p2, Drawable panel) {
        double slope = Supplier.calculateSlope(p1, p2);
        double finalSlope = Supplier.calculateAbsValue(slope);
        Color originalColor = p1.getColor();
        double currentYValue = p1.getY();
        double currentXValue = p1.getX();

        if (finalSlope <= MIDANGLE) {
            for (int x = p1.getIntX() + 1; x <= p2.getIntX()-1; x++) {
                double fractional = fractionPart(currentYValue);
                double rFractional = remainFractionPart(currentYValue);
                panel.setPixel(x, integerPart(currentYValue), 0.0, adjustOpacity(originalColor, rFractional).asARGB());
                panel.setPixel(x, integerPart(currentYValue) - 1, 0.0, adjustOpacity(originalColor, fractional).asARGB());
                currentYValue += finalSlope;
            }

        } else {
            for (int y = p1.getIntY() + 1; y <= p2.getIntY()-1; y++) {
                double fractional = fractionPart(currentXValue);
                double rFractional = remainFractionPart(currentXValue);

                panel.setPixel(integerPart(currentXValue), y, 0.0, adjustOpacity(originalColor, rFractional).asARGB());
                panel.setPixel(integerPart(currentXValue) - 1, y, 0.0, adjustOpacity(originalColor, fractional).asARGB());
                currentXValue += finalSlope;
            }

        }

    }

    public static LineRenderer make() {
        return new AnyOctantLineRenderer(new WuLineRenderer());
    }

    private int integerPart(double x) {
        return (int) x;
    }

    private double fractionPart(double x) {
        return x - integerPart(x);
    }

    private double remainFractionPart(double x) {
        return 1.0 - fractionPart(x);
    }

    private Color adjustOpacity(Color color, double fractional) {
        double r = color.getR() * (1 - fractional);
        double g = color.getG() * (1 - fractional);
        double b = color.getB() * (1 - fractional);

        return new Color(r, g, b);
    }
}
