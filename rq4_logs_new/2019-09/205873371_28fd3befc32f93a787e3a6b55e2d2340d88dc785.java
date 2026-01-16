package com.kodilla.game.creatures;

public class BlackDragon implements Creature {
    private int currentHealth = 20;
    private int power = 15;
    private String name = "Czarny smok";
    private String source = "pics/blackDragon.jpg";

    public String getSource() {
        return source;
    }

    public void setCurrentHealth(int currentHealth) {
        this.currentHealth = currentHealth;
    }

    public int getCurrentHealth() {
        return currentHealth;
    }

    public int getPower() {
        return power;
    }

    public String getName() {
        return name;
    }

    public void attack() {
    }
}