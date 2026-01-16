package com.cbfacademy.cars;

public class Car {
    public String make;
    public String model;
    public int year;
    

    public Car(String make, String model, int year){
        this.make = make;
        this.model = model;
        this.year = year;
    }

    public String toString(){
        String newCar = make + "\n"+model + "\n" + year;
        return newCar;
    }

    public void getMake(){
        System.out.println("Have you seen my nice new " +this.make +"?");
    }

    public void getModel(){
        System.out.println("It's a " +this.model+ " You know");
    }

    public void getYear(){
        System.out.println("It was released in " + this.year + ".");
    }

    public void getDetals(){
        getMake();
        getModel();
        getYear();
    }

}