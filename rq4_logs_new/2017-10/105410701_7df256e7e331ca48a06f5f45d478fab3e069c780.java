package com.playtika.family;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by jane on 9/22/17.
 */
public class Child extends FamilyMembers {
    private static final Logger LOG = LogManager.getLogger(FamilyMembers.class);
    private int hungerLevel = 1;
    private int satisfactionLevel = 1;

    public Child(String familyRole) {
        super(familyRole);
    }

    @Override
    public void play(int time) {
        satisfactionLevel += time;
    }

    public void eat(int food) {
        hungerLevel += food;
    }

    public void sayIfHungry(){
        if (hungerLevel < 5) {
            LOG.info("I'm hungry");
        }
        else LOG.info("I'm full");
    }

    public void sayIfBored(){
        if (satisfactionLevel < 5) {
            LOG.info("I'm bored");
        }
        else LOG.info("I'm happy");
    }
}