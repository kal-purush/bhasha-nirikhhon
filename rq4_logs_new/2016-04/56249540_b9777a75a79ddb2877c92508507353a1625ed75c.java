package CanvasToDraw.WithShape.shape;

import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.util.List;
import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.util.ArrayList;

/**
 *
 * @author ΙΩΑΝΝΑ
 */
public class NewRectangle implements IRectangleV2{
    private final List<Point2D> iziMode = new ArrayList<>();
    private final Rectangle2D r = new Rectangle2D.Double();
    private final int SIZE;
    
    public NewRectangle(Point2D p1, Point2D p2, int size){
        iziMode.add(p1);        
        iziMode.add(p2);
        SIZE = size;    
    }
    
    @Override
    public void doDrawing(Graphics g) {
        
        Graphics2D g2 = (Graphics2D)g;
        
        for (Point2D point : iziMode) {//to mikro tetragwno
            //einai to kentro tou mikrou tetragwnou
            double x = point.getX() - SIZE / 2;
            double y = point.getY() - SIZE / 2;
            r.setFrame(x, y, SIZE, SIZE);//to Frame tou orizei ti megethos tha exei 
            g2.fill(r);
        }

        //dimiourgite kai ena tetragwno.gi auto einai ana 2         
        r.setFrameFromDiagonal(iziMode.get(0),iziMode.get(1));
        g2.setPaint(new Color(0, 0, 100));// to xrwmma pou tha exei to tetragwno
        g2.fill(r);//gemizei to tetragwno
        g2.draw(r);//kanei to perigramma tou tetragwnou
    }    
}