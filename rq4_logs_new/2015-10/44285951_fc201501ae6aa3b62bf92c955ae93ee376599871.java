/*
 * Name:    Lewis Confair and Michael Whitley
 * File:    Food.java
 * Purpose: Provides an interface for an entity with color, direction, location
 *          and grid.
 */

package info.gridworld.actor;

import java.awt.Color;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author lconfair
 */

    

/**
 * A <code>Rock</code> is an actor that does nothing. It is commonly used to
 * block other actors from moving. <br />
 * The API of this class is testable on the AP CS A and AB exams.
 */

public class Food extends Actor
{    
    private static final Color DEFAULT_COLOR = Color.BLACK;

    /**
     * Constructs a black rock.
     */
    public Food()
    {
        setColor(DEFAULT_COLOR);
    }

    /**
     * Constructs a rock of a given color.
     * @param rockColor the color of this rock
     */
    public Food(Color foodColor)
    {
        setColor(foodColor);
    }

    /**
     * Overrides the <code>act</code> method in the <code>Actor</code> class
     * to do nothing.
     */
    public void act()
    {
    }
}