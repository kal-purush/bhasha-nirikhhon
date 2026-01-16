package CanvasToDraw.WithShape.shape;

import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Point;
import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;

/**
 *
 * @author ΙΩΑΝΝΑ
 */
public class MakeRectangle {
    Rectangle2D r = new Rectangle2D.Double();
    int SIZE = 8;


    public void makeRec(Point2D[] points , Graphics g) {
        Graphics2D g2 = (Graphics2D) g;
        
        for (Point2D point : points) {//to mikro tetragwno
            //einai to kentro tou mikrou tetragwnou
            double x = point.getX() - SIZE / 2;
            double y = point.getY() - SIZE / 2;
            r.setFrame(x, y, SIZE, SIZE);//to Frame tou orizei ti megethos tha exei
            g2.fill(r);
        }

        for (int i = 0; i < points.length; i = i + 2) { //me ti kathe epanalipsi 
            //dimiourgite kai ena tetragwno.gi auto einai ana 2         
            r.setFrameFromDiagonal(points[i], points[i + 1]);
            g2.setPaint(new Color(0, 0, 100));// to xrwmma pou tha exei to tetragwno
            g2.fill(r);//gemizei to tetragwno
            g2.draw(r);//kanei to perigramma tou tetragwnou

        }
    }

}