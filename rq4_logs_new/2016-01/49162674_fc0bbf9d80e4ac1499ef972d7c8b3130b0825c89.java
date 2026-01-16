import java.awt.*;

/**
 * Created by Damian Suski on 1/6/2016.
 */
public class Gem implements entity{
 
    private double x,y,h,w;

    public Gem(double x, double y, double h, double w)
    {
        this.x=x;
        this.y=y;
        this.h=h;
        this.w=w;
    }

    @Override
    public double getX() {
        return x;
    }

    @Override
    public double getY() {
        return y;
    }

    @Override
    public double getH() {
        return h;
    }

    @Override
    public double getW() {
        return w;
    }

    @Override
    public void setX(double d) {
        this.x = d;
    }

    @Override
    public void setY(double d) {
        this.x = d;
    }

    @Override
    public void update() {

    }

    @Override
    public void draw(Graphics g) {
        g.setColor(Color.PINK);
        g.fillRect((int)x,(int)y,(int)w,(int)h);
    }
}