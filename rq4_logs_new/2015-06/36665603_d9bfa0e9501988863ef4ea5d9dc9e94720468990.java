package com.example.finlay.manogotchi;

/**
 * Created by finlay on 01/06/15.
 */
public class Person {
    private int tiredness;
    private int hunger;
    private int happiness;
    private int fitness;
    private int age;
    private boolean asleep;

    public enum food {APPLE, BANANA, CHOCOLATE};

    public Person(int tiredness, int hunger, int happiness, int fitness, int age, boolean asleep) {
        this.tiredness = tiredness;
        this.hunger = hunger;
        this.happiness = happiness;
        this.fitness = fitness;
        this.age = age;
        this.asleep = asleep;
    }

    public Person() {
        tiredness = 0;
        hunger = 0;
        happiness = 100;
        fitness = 70;
        age = 0;
        asleep = false;
    }

    public void sleep(int time) {
        if (time + t)
    }

    public void consume(String food) {

    }

    public int getAge() {
        return age;
    }

    public int getFitness() {
        return fitness;
    }

    public int getHunger() {
        return hunger;
    }

    public int getHappiness() {
        return happiness;
    }

    public int getTiredness() {
        return tiredness;
    }

    public void setTiredness(int tiredness) {
        this.tiredness = tiredness;
    }

    public void setHunger(int hunger) {
        this.hunger = hunger;
    }

    public void setHappiness(int happiness) {
        this.happiness = happiness;
    }

    public void setFitness(int fitness) {
        this.fitness = fitness;
    }

    public void setAge(int age) {
        this.age = age;
    }
}