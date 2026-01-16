package edu.ucsb.cs56.w16.drawings.drewtaylor.advanced;

import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.util.Random;

/**
 * A main class to view an animation
 *
 * @author Drew Taylor
 * @version for CS56, W16
 */


public class AnimatedPictureViewer {

    private DrawPanel panel = new DrawPanel();
    
    private Melody melody = new Melody(100, 100, 200, 100, 1, 3, 5);
    
    Thread anim;   
    
    private int x = 100;
    private int y = 100;

    private int size_x = 200;
    private int size_y = 100;
    
    private int dx = 5;
    private int dy = 5;

    private int dsize_x = 5;
    private int dsize_y = 5;

    private int note1 = 1;
    private int note2 = 3;
    private int note3 = 5;

    private int dnote1 = 1;
    private int dnote2 = 3;
    private int dnote3 = 2;

    public static void main (String[] args) {
      new AnimatedPictureViewer().go();
    }

    public void go() {
      JFrame frame = new JFrame();
      frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

      frame.getContentPane().add(panel);
      frame.setSize(640,480);
      frame.setVisible(true);

      frame.setTitle("Drew Taylor's Animated Melody");

      frame.getContentPane().addMouseListener(new MouseAdapter() {
        public void mouseEntered(MouseEvent e){
        System.out.println("mouse entered");
          anim = new Animation();
          anim.start();
        }

        public void mouseExited(MouseEvent e){        
          System.out.println("Mouse exited");
          // Kill the animation thread
          anim.interrupt();
          while (anim.isAlive()){}
          anim = null;         
          panel.repaint();        
        }
      });
      
    } // go()

    class DrawPanel extends JPanel {
       public void paintComponent(Graphics g) {

        Graphics2D g2 = (Graphics2D) g;

         // Clear the panel first
          g2.setColor(Color.white);
          g2.fillRect(0,0,this.getWidth(), this.getHeight());

          // Draw the Ipod
          g2.setColor(Color.BLUE);
          Melody test = new Melody(x, y, size_x, size_y, note1, note2, note3);
          g2.draw(test);
       }
    }
    
    class Animation extends Thread {
      public void run() {
        try {
          while (true) {
            // Bounce off the walls

            if (x >= 400) { dx = -5; }
            if (x <= 50) { dx = 5; }
            
	    if (y >= 300) { dy = -5; }
	    if (y <= 100) { dy = 5; }

	    y += dy;
            x += dx;

	    // Size increase/decrease

	    if (size_x >= 250) { dsize_x = -5; }
	    if (size_x <= 150) { dsize_x = 5; }

	    if (size_y >= 150) { dsize_y = -5; }
	    if (size_y <= 50) { dsize_y = 5; }

	    size_x += dsize_x;
	    size_y += dsize_y;

	    // Change note locations

	    if (note1 >= 8) { dnote1 = -1; }
	    if (note1 <= 0) { dnote1 = 1; }

	    if (note2 >= 8) { dnote2 = -3; }
	    if (note2 <= 0) { dnote2 = 3; }
	    
	    if (note3 >= 8) { dnote3 = -2; }
	    if (note3 <= 0) { dnote3 = 2; }

	    note1 += dnote1;
	    note2 += dnote2;
	    note3 += dnote3;
     
            panel.repaint();
            Thread.sleep(50);
          }
        } catch(Exception ex) {
          if (ex instanceof InterruptedException) {
            // Do nothing - expected on mouseExited
          } else {
            ex.printStackTrace();
            System.exit(1);
          }
        }
      }
    }
    
}