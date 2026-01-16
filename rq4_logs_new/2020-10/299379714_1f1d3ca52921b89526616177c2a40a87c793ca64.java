package shapes;

public abstract class Quadrilateral extends Shape implements Measurable{

    protected double length;
    protected double width;

    public Quadrilateral(double aLength, double aWidth){
        this.length = aLength;
        this.width = aWidth;
    }

    public abstract void setLength(double aLength);

    public abstract void setWidth(double aWidth);

    public double getLength(){
        return this.length;
    }

    public double getWidth(){
        return this.width;
    };

}