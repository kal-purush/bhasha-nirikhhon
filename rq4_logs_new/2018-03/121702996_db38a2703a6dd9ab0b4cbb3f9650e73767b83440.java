package edu.ucsb.cs56.projects.game.alva.entity;

import edu.ucsb.cs56.projects.game.alva.Util.Vector2;

import edu.ucsb.cs56.projects.game.alva.entity.Entity;
import edu.ucsb.cs56.projects.game.alva.entity.Robot;
import edu.ucsb.cs56.projects.game.alva.Assets;
import java.awt.Graphics2D;

/**Creates an instance of a finish object and enables interaction between an instance of a robot and an instance of a finish 
 */
public class Finish extends Entity {

    Robot r;
    
    /**Calls the Entity Class constructor to set values and creates a reference to a rectangle object, and creates a reference to a Robot object
     * @param position pass in object of type Vector2 to set initial position of Finish object
     */
    public Finish(Vector2 position) {
        super(position, new Vector2(0, 0), 100, 100);
        r = Robot.getInstance();
    }
    
 
    
    /**Override the inherited drawIt function to draw finish object
     * @param win Graphics2D object that represents the window object for drawing to a GUI
     */
    @Override
    public void drawIt(Graphics2D win) {
        Assets.finish.draw(this, win);
    }
    
    /**
     * @param win Graphics2D object that represents the window object for drawing to a GUI
     
    @Override
    public void update(Graphics2D win) {
        super.update(win);
    }
    */
    
}