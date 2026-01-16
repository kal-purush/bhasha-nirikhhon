package com.cbfacademy.shapes;

public class Paint {
    
    private double coverage;

    public void paint(double coverage){
        this.setCoverage(coverage);
    }

    protected double getCoverage(){
        return coverage;
    }

    public void setCoverage(double coverage){
        this.coverage = coverage;
    }

    public double amount(Shape, shape){
        System.out.println(getShapeName() +"Will use "+Shape+" gallons of paint to cover it.");
    }

    public void painShapes(){
        
    }
}