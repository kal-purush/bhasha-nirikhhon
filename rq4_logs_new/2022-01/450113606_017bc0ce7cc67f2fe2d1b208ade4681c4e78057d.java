package com.example.polymorphism;

public class Calculate {

    String name="abc";

    public int multiply(int a,int b){
        System.out.println("both the inputs are integers and output also integer::::"+a*b);
        return  a*b;
    }

    public double multiply(double a,int b){
        System.out.println("first param is double and second parma is int and output is double:::"+a*b);
        return  a*b;
    }

    public double multiply(double a,double b){
        System.out.println("both the inputs are double and output also double::::"+a*b);
        return  a*b;
    }

    public int multiply(int a,double b){
        System.out.println("first param is int and second parma is double but output is int:::"+a*b);
        return  (int)(a*b);
    }

}