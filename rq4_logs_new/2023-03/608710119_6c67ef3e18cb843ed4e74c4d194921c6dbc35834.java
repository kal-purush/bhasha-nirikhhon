package Lesson1.impl;

import Lesson1.Animal;
import Lesson1.Flyable;
import Lesson1.Illable;

public class Duck extends Animal implements Flyable, Illable {
    public Duck(String name, String color) {
        super(name, color,2);
    }

    public Duck(String name) {
        this(name, null);
    }

    @Override
    public void speak() {
        System.out.println("Quack!");
    }

    @Override
    public void hunt() {

    }

    public void fly() {
        System.out.println("Полетели!");
    }

    @Override
    public int getFlightSpeed() {
        return 10;
    }

    @Override
    public void getIll() {

    }
}