import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.util.Random;

import javax.swing.JFrame;
import javax.swing.JPanel;

import java.awt.Polygon;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

class Pallete {
    static final Color Primary = Color.decode("#0f1a2e");
    static final Color Secondary = Color.decode("#0e334e");
    static final Color Accent = Color.decode("#933a39");
    static final Color DownLight = Color.decode("#dc9261");
    static final Color background = Color.decode("#1e4159");
    static final Color highlight = Color.decode("#4389d3");

    static Color getShade(Color color, int opacity) {
        return new Color(color.getRed(), color.getGreen(), color.getBlue(), opacity);
    }

    static Color getGradient(Color color1, Color color2, double value) {
        // from color1 to color2 by percent
        int result_red = (int)(color1.getRed() + ((value / 100) * (color2.getRed() - color1.getRed())));
        int result_green = (int)(color1.getGreen() + ((value / 100) * (color2.getGreen() - color1.getGreen())));
        int result_blue = (int)(color1.getBlue() + ((value / 100) * (color2.getBlue() - color1.getBlue())));

        // convert rgb to hex
        return Color.decode(String.format("#%02x%02x%02x", result_red, result_green, result_blue));
    }
}

class Graphicer extends GraphicsSwing {
    static Random random = new Random(1);
    static List<Integer> yCoordinates = new ArrayList<>();

    static Integer y_bresenham(int i) {
        return yCoordinates.get(i);
    }

    static void bresenhamLine(Graphics g, int x1, int y1, int x2, int y2, int size) {

        int dx = Math.abs(x2 - x1);
        int dy = Math.abs(y2 - y1);

        int sx = (x1 < x2) ? 1 : -1;
        int sy = (y1 < y2) ? 1 : -1;
        boolean isSwap = false;

        if (dy > dx) {
            int temp = dx;
            dx = dy;
            dy = temp;
            isSwap = true;
        }

        int D = 2 * dy - dx;
        int x = x1;
        int y = y1;

        for (int i = 1; i < dx; i++) {
            yCoordinates.add(y); // Store the y-coordinate

            plot(g, x, y, size);

            if (D >= 0) {
                if (isSwap)
                    x += sx;
                else
                    y += sy;
                D -= 2 * dx;
            }

            if (isSwap)
                y += sy;
            else
                x += sx;

            D += 2 * dy;
        }
    }

    static void bezierCurve(Graphics g, int x1, int y1, int x2, int y2, int x3, int y3, int x4, int y4) {
        for (int i = 0; i < 1000; i++) {
            float t = i / 1000.0f;
            plot(
                    g,
                    (int) (Math.pow(1 - t, 3) * x1
                            + 3 * t * Math.pow(1 - t, 2) * x2
                            + 3 * t * t * (1 - t) * x3
                            + Math.pow(t, 3) * x4),
                    (int) (Math.pow(1 - t, 3) * y1
                            + 3 * t * Math.pow(1 - t, 2) * y2
                            + 3 * t * t * (1 - t) * y3
                            + Math.pow(t, 3) * y4),
                    1);
        }
    }

    static BufferedImage floodFill(BufferedImage m, int x, int y, Color targetColor, Color replacementColor) {
        Queue<NodeCoordinate> q = new LinkedList<>();
        coloredPlot(m.getGraphics(), x, y, replacementColor);
        q.add(new NodeCoordinate(x, y));
        while (!q.isEmpty()) {
            NodeCoordinate cur = q.poll();
            try {
                // South
                if (m.getRGB(cur.getX(), cur.getY() + 1) == targetColor.getRGB()) {
                    coloredPlot(m.getGraphics(), cur.getX(), cur.getY() + 1, replacementColor);
                    q.add(new NodeCoordinate(cur.getX(), cur.getY() + 1));
                }
                // North
                if (m.getRGB(cur.getX(), cur.getY() - 1) == targetColor.getRGB()) {
                    coloredPlot(m.getGraphics(), cur.getX(), cur.getY() - 1, replacementColor);
                    q.add(new NodeCoordinate(cur.getX(), cur.getY() - 1));
                }
                // West
                if (m.getRGB(cur.getX() - 1, cur.getY()) == targetColor.getRGB()) {
                    coloredPlot(m.getGraphics(), cur.getX() - 1, cur.getY(), replacementColor);
                    q.add(new NodeCoordinate(cur.getX() - 1, cur.getY()));
                }
                // East
                if (m.getRGB(cur.getX() + 1, cur.getY()) == targetColor.getRGB()) {
                    coloredPlot(m.getGraphics(), cur.getX() + 1, cur.getY(), replacementColor);
                    q.add(new NodeCoordinate(cur.getX() + 1, cur.getY()));
                }
            } catch (ArrayIndexOutOfBoundsException e) {

            }
        }
        return m;
    }

    static void coloredPlot(Graphics g, int x, int y, Color c) {
        g.setColor(c);
        g.fillRect(x, y, 1, 1);
        g.setColor(Color.BLACK);
    }

    static class NodeCoordinate {
        private int x;
        private int y;

        NodeCoordinate(int x, int y) {
            this.x = x;
            this.y = y;
        }

        public int getX() {
            return x;
        }

        public int getY() {
            return y;
        }
    }

    static void drawPolygon(Graphics g, int[] x, int[] y) {
        Polygon poly = new Polygon(x, y, x.length);
        g.fillPolygon(poly);
    }

    static void drawRect(Graphics g, int x1, int y1, int x2, int y2, Color color) {
        int[] xTopPoly = { x1, x2, x1 };
        int[] yTopPoly = { y1, y1, y2 };

        int[] xBottomPoly = { x1, x2, x2 };
        int[] yBottomPoly = { y2, y1, y2 };
        Polygon topPoly = new Polygon(xTopPoly, yTopPoly, 3);
        Polygon bottomPoly = new Polygon(xBottomPoly, yBottomPoly, 3);

        g.setColor(color);
        g.fillPolygon(topPoly);
        g.fillPolygon(bottomPoly);
    }

    static void drawRect_Affine(Graphics g, int[] x, int[] y, Color color) {
        int[] xTopPoly = { x[0], x[1], x[2] };
        int[] yTopPoly = { y[0], y[1], y[2] };

        int[] xBottomPoly = { x[2], x[1], x[3] };
        int[] yBottomPoly = { y[2], y[1], y[3] };
        Polygon topPoly = new Polygon(xTopPoly, yTopPoly, 3);
        Polygon bottomPoly = new Polygon(xBottomPoly, yBottomPoly, 3);

        g.setColor(color);
        g.fillPolygon(topPoly);
        g.fillPolygon(bottomPoly);
    }

    static void drawEclipse(Graphics g, int centerX, int centerY, int width, int height, Color color) {
        g.setColor(color);
        int ellipseWidth = width / 2;
        int ellipseHeight = height / 2;

        int x = (width - ellipseWidth) / 2;
        int y = (height - ellipseHeight) / 2;

        int sides = 600; // Adjust the number of sides for smoother ellipse
        int[] xPoints = new int[sides];
        int[] yPoints = new int[sides];

        for (int i = 0; i < sides; i++) {
            double angle = 2 * Math.PI * i / sides;
            xPoints[i] = (int) (centerX + x + ellipseWidth * Math.cos(angle));
            yPoints[i] = (int) (centerY + y + ellipseHeight * Math.sin(angle));
        }

        g.fillPolygon(xPoints, yPoints, sides);
    }

    static void drawCircle(Graphics g, int centerX, int centerY, int radius, Color color) {
        int sides = 360;
        int[] xPoints = new int[sides];
        int[] yPoints = new int[sides];

        for (int i = 0; i < sides; i++) {
            double theta = 2 * Math.PI * i / sides;
            xPoints[i] = (int) (centerX + radius * Math.cos(theta));
            yPoints[i] = (int) (centerY + radius * Math.sin(theta));
        }

        Polygon circle = new Polygon(xPoints, yPoints, sides);
        g.setColor(color);
        g.fillPolygon(circle);
    }

    static void fourSide(Graphics g, int centerX, int centerY, int radius, Color color) {
        int sides = 4;
        int[] xPoints = new int[sides];
        int[] yPoints = new int[sides];

        for (int i = 0; i < sides; i++) {
            double theta = 2 * Math.PI * i / sides;
            xPoints[i] = (int) (centerX + radius * Math.cos(theta));
            yPoints[i] = (int) (centerY + radius * Math.sin(theta));
        }

        Polygon circle = new Polygon(xPoints, yPoints, sides);
        g.setColor(color);
        g.fillPolygon(circle);
    }

    static void drawCloudLine(Graphics g, int x1, int y1, int x2, int y2, int radius, Color color) {
        for (double j = 0; j <= 1; j += 0.05) {
            int dx = (int) ((x2 - x1) * j);
            int dy = (int) ((y2 - y1) * j);
            Graphicer.drawCircle(g, x1 + dx, y1 + dy, 15,
                    Pallete.getShade(color, 4));
        }
    }

    static void drawLightShade(Graphics g, int x1, int y1, int x2, int y2, int radius) {
        for (double j = 0; j <= 1; j += 0.02) {
            int dx = (int) ((x2 - x1) * j);
            int dy = (int) ((y2 - y1) * j);
            fourSide(g, x1 + dx, y1 + dy, 10,
                    Pallete.getShade(Pallete.Accent, 5));
        }
    }

    static void drawLightShade_1(Graphics g, int x1, int y1, int x2, int y2, int radius) {
        for (double j = 0; j <= 1; j += 0.02) {
            int dx = (int) ((x2 - x1) * j);
            int dy = (int) ((y2 - y1) * j);
            Graphicer.drawCircle(g, x1 + dx, y1 + dy, 4,
                    Pallete.getShade(Pallete.Accent, 20));
        }
    }

    static void plot(Graphics g, int x, int y, int size) {
        g.fillRect(x, y, size, size);
    }
}

class GraphicsSwing extends JPanel implements Runnable {

    protected Random random = new Random();

    protected double lastTime = System.currentTimeMillis();
    protected double currentTime, elapsedTime;
    protected double startTime = lastTime;
    protected double bridgeMove = 0;
    protected double poleMove = 0;
    protected double bridge_velocity = 100;
    protected double pole_velocity = 50;
    protected double person_velocity = 30;
    protected double personMove = 0;
    protected double comet_velocity = -8;
    protected double cometMove = 0;
    protected int tail_cap = 200;
    protected int trigger_a = 0;

    public void run() {
        while (true) {
            // time initiate
            currentTime = System.currentTimeMillis();
            elapsedTime = currentTime - lastTime;
            lastTime = currentTime;

            // bridge move
            bridgeMove += bridge_velocity * elapsedTime / 10000.0;
            poleMove += pole_velocity * elapsedTime / 3000.0;
            personMove += person_velocity * elapsedTime / 3000.0;
            cometMove += comet_velocity * elapsedTime / 1000.0;
            tail_cap += 2 * elapsedTime / 20.0;
            if (Math.abs(startTime - currentTime) >= 20000) {
                bridge_velocity = 0;
                pole_velocity = 0;
                comet_velocity = 0;
                trigger_a = 1;
            }
            else if(Math.abs(startTime - currentTime) >= 5000) {
                person_velocity = 0;
            }

            // Display
            repaint();
        }
    }

    public static void main(String[] args) {
        GraphicsSwing m = new GraphicsSwing();

        JFrame f = new JFrame();
        f.add(m);
        f.setTitle("Lonely night");
        f.setSize(600, 600);
        f.setBackground(Color.BLACK);
        f.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        f.setVisible(true);

        (new Thread(m)).start();
    }

    @Override
    public void paintComponent(Graphics g) {
        BufferedImage buffer = new BufferedImage(600, 600,
                BufferedImage.TYPE_INT_ARGB);
        Graphics2D g2 = buffer.createGraphics();

        paintBackground(g2);
        drawCloud(g2);
        drawComet(g2);
        drawStar(g2);
        drawElectricPole(g2);
        paintObject(g2);;
        paintBridge(g2);
        if (trigger_a == 1)
            paintCometFlare(g2);
        g2.setColor(Color.WHITE);
        g.drawImage(buffer, 0, 0, null);
    }

    private void paintCometFlare(Graphics2D g2)
    {
        Graphicer.drawEclipse(g2, 125, -235, 100, 80, Pallete.getShade(Pallete.highlight.darker().darker(), 70));
        Graphicer.drawEclipse(g2, 120, -227, 80, 50, Pallete.getShade(Pallete.highlight.darker(), 60));
        Graphicer.drawEclipse(g2, 125, -218, 50, 30, Pallete.getShade(Pallete.highlight, 50));
        Graphicer.drawEclipse(g2, 131, -212, 20, 10, Pallete.getShade(Pallete.highlight.brighter(), 40));
    }

    private void paintObject(Graphics2D g)
    {

        g.translate(personMove, 0);
        int[] xPointTorso = {115, 150, 105, 140};
        int[] yPointTorso = {430, 435, 500, 505};

        int[] xPointHead = {121, 154, 116, 145};
        int[] yPointHead = {395, 400, 450, 457};
        g.rotate(0.09);
        Graphicer.drawEclipse(g, 163, 380, 35, 50, Pallete.Primary);
        Graphicer.drawEclipse(g, 163, 412, 37, 20, Pallete.Primary);
        g.rotate(-0.09);
        Graphicer.drawRect_Affine(g, xPointTorso, yPointTorso, Pallete.Primary);
        Graphicer.drawRect_Affine(g, xPointHead, yPointHead, Pallete.Primary);
        g.translate(-personMove, 0);
    }

    private void paintBridge(Graphics2D g) {
        g.setColor(Pallete.Primary);
        int y_start = 350;
        int frence_y_offset = 470;
        g.translate(0, bridgeMove);

        //Light_pole
        Graphicer.drawCircle(g, 495, 425, 15, Pallete.Primary);
        int x_poleBase[] = { 483, 505, 478, 510 };
        int y_poleBase[] = { 425, 425, 500, 520 };
        Graphicer.drawRect_Affine(g, x_poleBase, y_poleBase, Pallete.Primary);
        Graphicer.bresenhamLine(g, 492, 450, 490, 110, 8);
        Graphicer.bresenhamLine(g, 490, 105, 418, 130, 5);
        Graphicer.yCoordinates.clear();
        Graphicer.drawCircle(g, 494, 110, 5, Pallete.Primary);
        Graphicer.drawEclipse(g, 415, 135, 30, 15, Pallete.DownLight);

        //light_shade
        int x_lightAffine[] = {
                                409, 435,
                            };
        int y_lightAffine =
                                142;
        for (int i = 0; i < 20; i+=2) {
            Graphicer.drawLightShade(g, x_lightAffine[0] - i, y_lightAffine + i, x_lightAffine[1] + i, y_lightAffine+ i, 100);
        }

        // top_2
        g.setColor(Pallete.Primary);
        for (int i = 0; i < 2; i++) {
            Graphicer.bresenhamLine(g, 0, y_start += 10, 600, frence_y_offset, 5);
            frence_y_offset += 10;
        }

        // low_0
        g.setColor(Pallete.Primary);
        Graphicer.bresenhamLine(g, 0, 410, 600, 500, 5);

        // stick_loop
        int x_start = 11;
        y_start = 372;
        int x_start_bot = 9;
        for (int i = 0; i < 600; i += 30) {
            if (i % 180 == 0)
                Graphicer.bresenhamLine(g, x_start + i, Graphicer.y_bresenham(i + 600) - 30, x_start_bot + i,
                        Graphicer.y_bresenham(i + 1200) + 50, 6);
            else
                Graphicer.bresenhamLine(g, x_start + i, Graphicer.y_bresenham(i + 600), x_start_bot + i,
                        Graphicer.y_bresenham(i + 1200), 4);
        }
        Graphicer.yCoordinates.clear();

        // Bridge_pillar0
        int x_pillarB[] = { 0, 600, 0, 600 };
        int y_pillarB[] = { 435, 511, 900, 900 };

        Graphicer.drawRect_Affine(g, x_pillarB, y_pillarB, Pallete.Secondary);
        // Bridge_pillar1
        int x_pillar1[] = { 80, 140, 70, 135 };
        int y_pillar1[] = { 490, 500, 900, 900 };

        // Bridge_pillar2
        int x_pillar2[] = { 260, 320, 250, 310 };
        int y_pillar2[] = { 510, 515, 900, 900 };

        // Bridge_pillar3
        int x_pillar3[] = { 417, 461, 415, 460 };
        int y_pillar3[] = { 530, 535, 900, 900 };

        int x_pillar[][] = { x_pillar1, x_pillar2, x_pillar3 };
        int y_pillar[][] = { y_pillar1, y_pillar2, y_pillar3 };

        for (int i = 0; i < y_pillar.length; i++) {
            Graphicer.drawRect_Affine(g, x_pillar[i], y_pillar[i], Pallete.Primary);
        }
        // Bridge_base floor
        int x_baseBridge[] = { 0, 600, 0, 600 };
        int y_baseBridge[] = { 435, 511, 490, 550 };
        Graphicer.drawRect_Affine(g, x_baseBridge, y_baseBridge, Pallete.Primary);
    }

    private void drawComet(Graphics2D g) {
        g.translate(cometMove, -cometMove/4);
        g.setColor(Pallete.getShade(Pallete.highlight, 15));
        for (int i = 0; i < tail_cap; i += 10) {
            // int[] x = { 245, 240, 450 + i };
            // int[] y = { 280, 285, 210 - (int) (i * 0.5) };
            int[] x = { 300, 295, 505 + i };
            int[] y = { 280, 285, 210 - (int) (i * 0.5) };
            Graphicer.drawPolygon(g, x, y);
        }
        g.translate(-cometMove, cometMove/4);
        repaint();
    }

    private void drawCloud(Graphics g) {
        int[] x1 = { 300, 330, 350, 360 };
        int[] y1 = { 400, 375, 340, 325 };
        int[] x2 = { 440, 450, 470, 530 };
        int[] y2 = { 500, 470, 460, 470 };
        int[] posX = { -60, 100, 70, -120, -330, 40, 50, 200 };
        int[] posY = { 20, -40, -60, 50, -200, -100, -70, -20 };
        for (int k = 0; k < posX.length; k++) {
            x1 = transfromArray(x1, +(posX[k]));
            y1 = transfromArray(y1, +(posY[k]));
            x2 = transfromArray(x2, +(posX[k]));
            y2 = transfromArray(y2, +(posY[k]));
            for (int i = 0; i < x1.length; i++)
                Graphicer.drawCloudLine(g, x1[i], y1[i], x2[i], y2[i], 15, Pallete.highlight);
        }
    }

    private int[] transfromArray(int[] a, int value) {
        int[] newA = new int[a.length];
        for (int i = 0; i < a.length; i++)
            newA[i] = a[i] + value;
        return newA;
    }

    private void drawStar(Graphics g) {
        int[] x = { 20, 70, 110, 65, 135, 280, 330, 385, 8, 25, 15,
                50, 360, 370, 410, 440, 445, 460, 550, 540, 580,
                560, 550, 570, 580 };
        int[] y = { 40, 20, 40, 82, 45, 30, 10, 35, 300, 320, 380,
                440, 420, 430, 330, 410, 420, 460, 380, 390, 400,
                310, 40, 20, 60 };
        for (int i = 0; i < x.length; i++)
            Graphicer.drawCircle(g, x[i], y[i], 2, Pallete.highlight);
    }

    private void drawElectricPole(Graphics2D g) {
        g.translate(0, poleMove);
        g.setColor(Pallete.Primary);
        Graphicer.bresenhamLine(g, 235, 390, 221, 520, 3);
        Graphicer.bresenhamLine(g, 220, 400, 250, 405, 3);
        Graphicer.bresenhamLine(g, 220, 410, 250, 415, 3);
        Graphicer.bezierCurve(g, 220, 400, 340, 400, 450, 420, 600, 320);
        Graphicer.bezierCurve(g, 250, 405, 340, 410, 450, 430, 600, 350);
        Graphicer.bezierCurve(g, 250, 415, 340, 420, 450, 440, 600, 380);
        Graphicer.yCoordinates.clear();
    }

    private void paintBackground(Graphics g) {
        for (int i = 0; i < 100; i++) {
            g.setColor(Pallete.getGradient(Pallete.background.darker(), Pallete.background, i));
            g.fillRect(0, 6 * i, 600, 6);
        }
    }
}