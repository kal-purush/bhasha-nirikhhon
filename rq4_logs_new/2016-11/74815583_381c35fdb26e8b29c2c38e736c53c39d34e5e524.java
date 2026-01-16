/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package mathmaticalshape.mathmaticalShape;

import static java.lang.Math.PI;

/**
 *
 * @author user
 */
public class Area {
    private double radius,length,breadth,height;

   

    public Area(double radius, double length, double breadth, double height) {
        this.radius = radius;
        this.length = length;
        this.breadth = breadth;
        this.height = height;
    }

    public double getRadius() {
        return radius;
    }

    public void setRadius(double radius) {
        this.radius = radius;
    }

    public double getLength() {
        return length;
    }

    public void setLength(double length) {
        this.length = length;
    }

    public double getBreadth() {
        return breadth;
    }

    public void setBreadth(double breadth) {
        this.breadth = breadth;
    }

    public double getHeight() {
        return height;
    }

    public void setHeight(double height) {
        this.height = height;
    }

    public Area() {
    }
    public void areaCircle(){
         double area;
         area=2*PI*radius;
         System.out.println("Area of Circle\t"+area);
                
    }
    public void areaRectangle(){
        double area;
        area=length*breadth;
        System.out.println("The area of rectangle\t"+area);
    }
    public void areaTriangle(){
        double area;
        area=(1/2)*breadth*height;
        System.out.println("Area of triangle is \t"+area);
    }
    public void areaSquare(){
        double area;
        area=length*length;
        System.out.println("The area of square"+area);
    }
    
}