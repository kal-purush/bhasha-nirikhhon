package ru.otus.java.basic.safarov.homework06;

public class Cat {
    private final String name;
    private final int appetite;
    private boolean satiety;

    public Cat(String name, int appetite) {
        this.name = name;
        this.appetite = appetite;
        this.satiety = false;
    }

    public String getName() {
        return name;
    }

    public int getAppetite() {
        return appetite;
    }

    public boolean isSatiety() {
        return satiety;
    }

    public void eat(int food){
        if (appetite > food){
            return;
        }
        satiety = true;
    }

    @Override
    public String toString() {
        return "Кот " + name + " " + (isSatiety()?"сыт":"голодный");
    }
}