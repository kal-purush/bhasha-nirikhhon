package com.tarasek.mateusz;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BombTest implements SharedVariables{

    Bomb bomb;

    @BeforeEach
    void setBomb(){
        bomb = new Bomb(10,20);
    }

    @Test
    void init(){
        assertNotNull(bomb.getImage());
        assertEquals(10, bomb.getX());
        assertEquals(20, bomb.getY());
    }

    @Test
    void isDestroyed(){
        assertFalse(bomb.isDestroyed());
    }

    @Test
    void setDestroyed(){
        bomb.setDestroyed(true);
        assertTrue(bomb.isDestroyed());
    }

    @Test
    void moveDownwards(){
        bomb.moveDownwards();
        assertEquals(20 + BOMB_SPEED, bomb.getY());
    }

}