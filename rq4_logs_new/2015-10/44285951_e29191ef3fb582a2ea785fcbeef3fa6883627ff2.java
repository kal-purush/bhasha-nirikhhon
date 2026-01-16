/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package info.gridworld.actor;

import info.gridworld.grid.Grid;
import info.gridworld.grid.Location;
import java.awt.Color;
import java.util.ArrayList;

/**
 *
 * @author lconfair
 */
public class Entrance extends Actor
{
    

    private static final Color DEFAULT_COLOR = Color.BLACK;

    /**
     * Constructs a black rock.
     */
    public Entrance()
    {
        setColor(DEFAULT_COLOR);
    }

    /**
     * Constructs a rock of a given color.
     * @param rockColor the color of this rock
     */
    public Entrance(Color rockColor)
    {
        setColor(rockColor);
    }

    /**
     * Overrides the <code>act</code> method in the <code>Actor</code> class
     * to do nothing.
     */
    public void act()
    {
        
        if (getGrid() == null)
            return;       
        processActors();
        
    }
    
    /**
     * Method:  getActors
     * Purpose: Gets the actors that are in the same grid for processing. 
     *          Implemented to return the actors that occupy neighboring grid 
     *          locations. 
     * @return a list of actors that are neighbors of this critter
     */
    public ArrayList<Actor> getActors()
    {
        return getGrid().getNeighbors(getLocation());
    }
    
    /**
     * Processes the actors. Implemented to "eat" (i.e. remove) all actors that
     * are not rocks or critters. Override this method in subclasses to process
     * neighbors in a different way. <br />
     * Precondition: All objects in <code>actors</code> are contained in the
     * same grid as this critter.
     * @param actors the actors to be processed
     */
    public void processActors()
    {
        Grid<Actor> gr = getGrid();        
              
        
        ArrayList<Location> L = getGrid().getOccupiedLocations();
        Actor player = null;
        for (Location i : L)
        {
            player = getGrid().get(i);
            if ( player instanceof Human)
                return;
        }
        
        ArrayList<Location> open = getGrid().getEmptyAdjacentLocations(getLocation());
        int n = open.size();
        if (n == 0)
            return;
        int r = (int) (Math.random() * n);
        
        Location place = open.get(r);
        Human h = new Human();
        h.putSelfInGrid(gr , place);
      
    }
}