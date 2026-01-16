package com.example.demo.classes;

public class Man implements Person{

    public String name;
    int age;
    public String city;
    public String country;

    public Man(String name, int age, String city, String country){
        this.age = age;
        this.city = city;
        this. country = country;
        this.name = name;
    }
    @Override
    public void introduce(String name){
        System.out.println("My name is : " + name);
    }

    @Override
    public void sayAge(int age){
        System.out.println("My age is : " + age);
    }

    @Override
    public void whereFrom(String city, String country){
        System.out.println("I am from " + city + "," + country);
    }

}