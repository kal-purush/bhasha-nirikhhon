package com.ua.robotdreams.homework15;

public class Driver extends Human{
    @Override
    public void canDo() {
        System.out.println("I can drive a car!");
    }

    @Override
    public void startMoving() {
        System.out.println("Put your seatbelts on! We're starting our trip!");
    }

    @Override
    public void moving() {
        System.out.println("We're moving pretty fast!");
    }

    @Override
    public void stopMoving() {
        System.out.println("We've got to the place of destination!");
        System.out.println("\n");
    }
}