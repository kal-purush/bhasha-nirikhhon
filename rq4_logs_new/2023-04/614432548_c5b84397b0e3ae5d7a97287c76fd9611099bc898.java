package org.example;

import org.example.homework_nr_8.Animal;

public class Elephant extends Erbivor implements Animal {

    @Override
    public void EatGrass() {System.out.println("Elephant is vegetarian");

    }

    @Override
    public void swim() {System.out.println("Elephant is swiming");

    }

    @Override
    public void eat_16_hours_per_day() {System.out.println("Elephant eats 16 hours every day");

    }

    @Override
    public void has_trunk() {System.out.println("Elephant has 2 big trunks");

    }

    @Override
    public void long_liver() {
        Animal.super.long_liver();
    }
}