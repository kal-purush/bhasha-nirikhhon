abstract class Shape {
    abstract public void Compute_Area(int a,int b);
    public void Calculate_Area() {
    }
}
class Triangle extends Shape {
    public int base;
    public int height;
    public void Compute_Area(int base,int height) {
        this.base=base;
        this.height=height;
    }
    public void Calculate_Area() {
        System.out.println("Area of Triangle is "+(0.5*(base*height)));
    }
}
class Rectangle extends Shape {
    public int length;
    public int breadth;
    public void Compute_Area(int length,int breadth) {
        this.length=length;
        this.breadth=breadth;
    }
    public void Calculate_Area() {
        System.out.println("Area of Rectangle is "+(length*breadth));
    }
}
public class Abstract {
    public static void main(String[] args) {
        Shape obj1=new Triangle();
        obj1.Compute_Area(30, 10);
        obj1.Calculate_Area();
        Shape obj2=new Rectangle();
        obj2.Compute_Area(9, 18);
        obj2.Calculate_Area();      
    }
}