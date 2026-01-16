package com.company;

import java.time.LocalDateTime;
import java.util.List;

public class Main {

    public static void main(String[] args) {
        double[] values = new double[100];
        for (int i = 0; i < 100; i++) {
            values[i] = Math.random() *100;
        }
        OtherClass stats = new OtherClass(values);

        System.out.println(stats.getSum());


        Rectangle firstRectangle = new Rectangle();
        firstRectangle.area();

        firstRectangle.setWidth(300);
        firstRectangle.setHeight(300);
        System.out.println("Height: "+firstRectangle.getHeight());
        System.out.println("Width: "+firstRectangle.getWidth());
        System.out.println("Area: "+firstRectangle.area());

        Square newSquare = new Square();

        System.out.format( "width: %.2f height: %.2f",
                newSquare.getWidth(), newSquare.getHeight());

        newSquare.setWidth(400);

        System.out.format(" width: %.2f height: %.2f",
                newSquare.getWidth(),newSquare.getHeight());

        Rectangle rect = new Square();
        rect.setWidth(350);
        System.out.format("\n width: %.2f height: %.2f ",rect.getWidth(),rect.getHeight());


//        password();
//        password("password12345");
//        password("password12345",5,false);
//
//        charAt()
//        indexOf()
//        replace()
//        subString()
//        toLowerCase()
//        toUpperCase()

    }


    public String getLocalTime(){
        return "invoked by any of the customer of the class";
    };


    public static String sayMyName(String name){
        return name;

    }


    private static String password(String password,double numberOfChar, boolean isPasswordCorrect){
        return "Only invoked within the class definition";
    }
    private static String password(double numberOfChar, boolean isPasswordCorrect){
        return "Only invoked within the class definition";
    }
    private static String password(int num, String username){
        return "Only invoked within the class definition";
    }
    private static String password(String password, int num){
        return "Only invoked within the class definition";
    }

    private static String password(){
        userName();
        return "Only invoked within the class definition";

    }



   static String myName(){
        return "means that the method can be invoked from within the class or " +
                "by any other class within the same package that contains this class.";
    }

    protected static String userName(){
        myName();
        return "invoked from within the class or from within any subclass of the class.";
    }
}