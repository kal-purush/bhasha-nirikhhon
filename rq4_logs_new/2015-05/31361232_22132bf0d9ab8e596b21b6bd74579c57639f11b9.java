package action;

import java.awt.*;
import java.awt.geom.AffineTransform;
import java.awt.geom.Rectangle2D;

import net.sf.jasperreports.engine.JRAbstractSvgRenderer;
import net.sf.jasperreports.engine.JRException;

/**
 * Draws a colored, circular gradient background used in the report.
 */
public class CircularGradientImageRenderer extends JRAbstractSvgRenderer {
    /**
     * The gradient color.
     */
    private String rgb;

    /**
     * Create a circular gradient image in a bounded square.
     *
     * @param rgb The gradient color.
     */
    public CircularGradientImageRenderer(String rgb) {
        this.rgb = rgb;
    }

    /**
     * Pull out a substring without returning null.
     *
     * @param value The value.
     * @param begin The beginning index.
     * @param end   The ending index.
     * @return The substring.
     */
    private static String safeSubstring(String value, int begin, int end) {
        String sub = "";

        if (value != null) {
            if (value.length() > begin) {
                end = Math.min(end, value.length() - 1);
                sub = value.substring(begin, end + 1);
            }
        }

        return sub;
    }

    /**
     * Parse a hex value without throwing an exception.
     *
     * @param value The value to convert.
     * @return The integer value.
     */
    private static int safeHexParse(String value) {
        try {
            return Integer.parseInt(value, 16);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    /**
     * Convert a string hex color into a Java Color object.
     *
     * @param rgb The string value.
     * @return The Color object.
     */
    public static Color stringToRGB(String rgb) {
        int r = 0;
        int g = 0;
        int b = 0;

        if (rgb != null && rgb.startsWith("#")) {
            r = safeHexParse(safeSubstring(rgb, 1, 2));
            g = safeHexParse(safeSubstring(rgb, 3, 4));
            b = safeHexParse(safeSubstring(rgb, 5, 6));
        }

        return new Color(r, g, b);
    }

    public void render(Graphics2D g2d, Rectangle2D rect) throws JRException {
        // Save the Graphics2D affine transform
        AffineTransform savedTrans = g2d.getTransform();

        float radius = (float) (Math.max(rect.getHeight(), rect.getWidth()) / 2);
        float[] fractions = {0.0f, 0.3f, 1.0f};
        Color[] colors = {Color.WHITE, Color.WHITE, stringToRGB(rgb)};

        // Paint a nice background...
        g2d.setPaint(new RadialGradientPaint((float) rect.getCenterX(), (float) rect.getCenterY(), radius, fractions, colors));
        g2d.fillRect(0, 0, (int) rect.getWidth(), (int) rect.getHeight());
        g2d.draw(rect);

        // Restore the Graphics2D affine transform
        g2d.setTransform(savedTrans);
    }
}