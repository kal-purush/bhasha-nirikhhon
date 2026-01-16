package edu.eitc;

import java.io.PrintStream;

public class Main {

    public static void main(String[] args) {
        Chicken.printDesription();
        System.out.println("Number of chickens: " + Chicken.chickenCount);

        Chicken tom = new Chicken("Tom");
        tom.printNameHasLegs();
        Chicken mary = new Chicken("Mary");
        mary.printNameHasLegs();

        Egg egg = tom.layEgg();
        System.out.println("Mother of the egg: " + egg.getMotherName());

        System.out.println("Number of chickens: " + Chicken.chickenCount);
    }
}