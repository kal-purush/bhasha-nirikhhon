package com.kodilla.game.engine;

public class Player {
    private int currentLife = 30;
    private int currentMana = 5;
    private String name;

    public Player(String name) {
        this.name = name;
    }

    public int getCurrentLife() {
        return currentLife;
    }

    public void setCurrentLife(int currentLife) {
        this.currentLife = currentLife;
    }

    public int getCurrentMana() {
        return currentMana;
    }

    public void setCurrentMana(int currentMana) {
        this.currentMana = currentMana;
    }

    public String getName() {
        return name;
    }

}