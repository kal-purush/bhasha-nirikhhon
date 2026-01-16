package edu.ucsb.cs56.w16.drawings.ttstarck.advanced;
import java.awt.Graphics;
import java.awt.Graphics2D;
import javax.swing.JPanel;
import javax.swing.JComponent;

// the four tools things we'll use to draw

import java.awt.geom.Line2D;  // single lines
import java.awt.geom.Ellipse2D;  // ellipses and circles
import java.awt.Rectangle;  // squares and rectangles
import java.awt.geom.GeneralPath; // combinations of lines and curves


import java.awt.geom.Rectangle2D; // for rectangles drawing with Doubles

import java.awt.Color; // class for Colors
import java.awt.Shape; // Shape interface
import java.awt.Stroke; // Stroke interface
import java.awt.BasicStroke; // class that implements stroke

import edu.ucsb.cs56.w16.drawings.utilities.ShapeTransforms;
import edu.ucsb.cs56.w16.drawings.utilities.GeneralPathWrapper;


/**
   A component that draws an animated picture by Jakob Staahl
   
   @author Tristan Starck
   @version CS56, W16
   
*/


public class AnimatedPictureComponent extends JComponent
{  
    private Shape clock;
    private double centerXPosition;
    private double centerYPosition;
    private double radius;
    private double angle = Math.PI/30;
    private int minute = 0;
    private int hour = 0;
    /** Constructs an AnimatedPictureComponent with specific properties.
	This animated picture depicts a clock with its hands rotating.

	@param centerXPosition the x coordinate of the center of the clock
	@param centerYPosition the y coordinate of the center of the clock
	@param radius the radius of the clock	
    */
    public AnimatedPictureComponent(double centerXPosition, double centerYPosition, double radius){
	this.centerXPosition = centerXPosition;
	this.centerYPosition = centerYPosition;
	this.radius = radius;

	clock = new ClockWithTicks(this.centerXPosition, this.centerYPosition, this.radius, this.minute, this.hour);
    }

    /** The paintComponent method is orverriden to display
	out animation. Each time this method is called, the
	hands of the clock are updated
     */
    
   public void paintComponent(Graphics g)
   {  
       Graphics2D g2 = (Graphics2D) g;
       
       g2.setColor(Color.BLACK); g2.draw(clock);

       this.minute++;;
       if(this.minute == 60){
	   this.minute = 0;
	   this.hour++;
       }
       if(this.hour == 12)
	   this.hour = 0;
      
       clock = new ClockWithTicks(this.centerXPosition, this.centerYPosition, this.radius, this.minute, this.hour);
   }    
  
}