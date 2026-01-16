package com.character;

import java.util.Random;

public class Zombie {
    Integer health;
    String location;  // until the locations are implemented

    // CONSTRUCTOR
    public Zombie(Integer health, String location) {
        this.health = health;
        this.location = location;
    }

    //METHODS
    public Integer getHealth() {
        return health;
    }

    public void setHealth(Integer health) {
        this.health = health;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public int attack(){
        return (int) (Math.random() * 5) + 1;
    }

    public void takeDamage(int damageTaken){
        int currentHp = getHealth() - damageTaken;
        setHealth(currentHp);
    }
}