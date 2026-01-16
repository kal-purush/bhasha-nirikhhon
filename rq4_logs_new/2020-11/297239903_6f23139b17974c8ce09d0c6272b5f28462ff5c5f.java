import java.util.Scanner;

abstract class Shapes {
    double length , breadth , radius ;
    final static double pi = 3.14 ;
    Scanner sc = new Scanner(System.in);
    abstract void printArea();
}
class Circle extends  Shapes {
    @Override
    void printArea() {
        System.out.println("radius of circle : ");
        radius = sc.nextInt();
        System.out.println("Area of Circle is : "+pi*radius*radius );
    }
}

class Rectangle extends  Shapes {
    @Override
    void printArea() {
        System.out.println("length and breadth of rectangle : ");
        length = sc.nextByte();
        breadth = sc.nextByte();
        System.out.println("Area of Rectangle is : " + length*breadth);
    }
}

class Triangle extends Shapes {
    @Override
    void printArea() {
        System.out.println("length and breadth of triangle : ");
        length = sc.nextByte();
        breadth = sc.nextByte();
        System.out.println("Area of triangle is : "+ 0.5 * length * breadth);
    }
}

public class Expt11 {
    public static void main(String[] args) {
       Rectangle r = new Rectangle();
       r.printArea();

       Triangle t = new Triangle();
       t.printArea();

       Circle c = new Circle();
       c.printArea();

    }
}