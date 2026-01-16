// WARNING: This file is auto-generated and any changes to it will be overwritten
import lang.stride.*;
import java.util.*;
import greenfoot.*;

/**
 * 
 */
public class Worm extends Actor
{

    /**
     * Act - do whatever the Worm wants to do. This method is called whenever the 'Act' or 'Run' button gets pressed in the environment.
     */
    public void act()
    {
        move(1);
        if (Greenfoot.isKeyDown("a")) {
            turn(1);
        }
        move(1);
        if (Greenfoot.isKeyDown("b")) {
            turn(1);
        }
        move(1);
        Actor Lobster = getOneObjectAtOffset(0, 0, Lobster.class);
        if (Lobster != null) {
            getWorld().removeObject(Lobster);
        }
    }
}