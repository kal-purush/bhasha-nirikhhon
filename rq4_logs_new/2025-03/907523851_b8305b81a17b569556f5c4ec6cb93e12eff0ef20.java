package src;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.event.MouseMotionListener;
import java.awt.geom.Rectangle2D;

public class Obstruction implements Runnable {
    private double x;
    private double y;

    private double sizeX = 160;
    private double sizeY = 80;

    private Color color;

    public Obstruction() {
        color = new Color((float) Math.random(), (float) Math.random(),
                (float) Math.random());
        
        Thread thisThread = new Thread(this);
        // Запускаем поток
        thisThread.start();
    }
    
    public class MouseMotionHandler implements MouseMotionListener {
        @Override
        public void mouseDragged(java.awt.event.MouseEvent e) {
            //Do nothing
        }
        
        @Override
        public void mouseMoved(java.awt.event.MouseEvent e) {
            x = e.getX();
            y = e.getY();
        }
    }

    @Override
    public void run() {
        
    }

    public void paint(Graphics2D canvas) {
        canvas.setColor(color);
        canvas.setPaint(color);
        Rectangle2D.Double rect = new Rectangle2D.Double(x - sizeX / 2, y - sizeY / 2, sizeX, sizeY);
        canvas.draw(rect);
        canvas.fill(rect);
    }
    
}