package com.company;

class Circle {
    private double radius = 1.0;
    private String color = "red";

    public Circle() {
    }

    public Circle(double radius) {
        this.radius = radius;
    }

//    public double getRadius(){
//        return this.radius;
//    }
//
//    public double getArea(){
//        double area = Math.pow(this.radius,2) * Math.PI;
//        return area;
//    }

    private double getRadius(){
        return this.radius;
    }

    private double getArea(){
        double area = Math.pow(this.radius,2) * Math.PI;
        return area;
    }
}

