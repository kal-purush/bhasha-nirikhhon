/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package info.gridworld.actor;

import info.gridworld.grid.Location;

import java.util.ArrayList;
/**
 *
 * @author Michael
 */
public class Monster extends Being 
{
    /**
     * Processes the actors. Implemented to "eat" (i.e. remove) all actors that
     * are not rocks or critters. Override this method in subclasses to process
     * neighbors in a different way. <br />
     * Precondition: All objects in <code>actors</code> are contained in the
     * same grid as this critter.
     * @param actors the actors to be processed
     */
    public void processActors(ArrayList<Actor> actors)
    {
        for (Actor a : actors)
        {
            if (!(a instanceof Rock) && !(a instanceof Monster))
                a.removeSelfFromGrid();
        }
    }

    /**
     * Gets the possible locations for the next move. Implemented to return the
     * empty neighboring locations. Override this method in subclasses to look
     * elsewhere for move locations.<br />
     * Postcondition: The locations must be valid in the grid of this critter.
     * @return a list of possible locations for the next move
     */
    public ArrayList<Location> getMoveLocations()
    {
        return getGrid().getEmptyAdjacentLocations(getLocation());
    }

    /**
     * Selects the location for the next move. Implemented to randomly pick one
     * of the possible locations, or to return the current location if
     * <code>locs</code> has size 0. Override this method in subclasses that
     * have another mechanism for selecting the next move location. <br />
     * Precondition: All locations in <code>locs</code> are valid in the grid
     * of this critter
     * @param locs the possible locations for the next move
     * @return the location that was selected for the next move.
     */
    public Location selectMoveLocation(ArrayList<Location> locs)
    {
        int n = locs.size();
        if (n == 0)
            return getLocation();
        int r = (int) (Math.random() * n);
        return locs.get(r);
    }

    /**
     * Moves this critter to the given location. Implemented to call moveTo.
     * Override this method in subclasses that want to carry out other actions
     * for moving (for example, turning or leaving traces). <br />
     * Precondition: <code>loc</code> is valid in the grid of this critter
     * @param loc the location to move to (must be valid)
     */
    public void makeMove(Location loc)
    {
        moveTo(loc);
    }
}