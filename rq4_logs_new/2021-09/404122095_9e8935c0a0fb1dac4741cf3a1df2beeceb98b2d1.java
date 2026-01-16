package com.character;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class ZombieTest {
    Zombie zombie;
    @Before
    public void setUp() throws Exception {
        zombie = new Zombie(6, "Landing Dock");
    }

    @Test
    public void getHealth() {
        assertEquals(6, (int)zombie.getHealth());
    }

    @Test
    public void setHealth() {
        zombie.setHealth(4);
        assertEquals(4, (int)zombie.getHealth());
    }

    @Test
    public void attack() {
        int attackPower = zombie.attack();
        assertTrue(attackPower >=1 && attackPower <= 5);
    }

    @Test
    public void takeDamage_healthShouldBePositive_whenDamageAppliedSmallerThanHealth() {
        zombie.takeDamage(1);
        assertEquals(5, (int)zombie.getHealth());
    }

    @Test
    public void takeDamage_healthShouldBeZero_whenDamageAppliedGreatThanHealth() {
        zombie.takeDamage(10);
        assertEquals(0, (int)zombie.getHealth());
    }
}