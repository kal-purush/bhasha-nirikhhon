package com.cbfacademy.shapes;

public class Cylinder extends Shape {
    
    private double height;
    private double radius;

    public Cylinder(String shapeName, double height, double radius){
        super(shapeName);
        this.setHeight(height);
        this.setRadius(radius);
    }

        protected double getHeight(){
            return height;
        }

        protected double getRadius(){
            return radius;
        }

        public double setHeight( double height){
            return this.height = height;
        }

        public double setRadius(double radius){
            return this.radius = radius;
        }

        @Override
        public double area(){
            double cylinderArea = 2 * Math.PI * radius * height;
            System.out.println("The area of the "+ getShapeName()+ " is "+ cylinderArea +".");
            return cylinderArea;
            
        }
}