/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package smallComponenets;

import java.awt.Color;
import java.awt.GradientPaint;
import java.awt.Graphics;
import java.awt.Graphics2D;
import javax.swing.JButton;
import javax.swing.JToggleButton;

/**
 *
 * @author Christopher
 */
public class smallBTN_TG extends JToggleButton{
      
      int W;
      int H;
      GradientPaint GP;
      
      public static final int blueWhite = 0;
      public static final int redWhite = 1;
      public static final int greenWhite = 2;
      public static final int yellowWhite = 3;
      
      @Override
      public void paintComponent(Graphics g){
            Graphics2D g2 = (Graphics2D) g;
            g2.setPaint(GP);
            g2.fillRect(0, 0, getWidth(), getHeight());
            
            super.paintComponents(g);
      }
      
      public smallBTN_TG(int w, int h, int colorGradient){
            //W = w;
            //H = h;
            if(colorGradient == 0){
                  GP = new GradientPaint(0,0, Color.BLUE, w, h, Color.WHITE);
            }else if(colorGradient == 1){
                  GP = new GradientPaint(0,0, Color.RED, w, h, Color.WHITE);
            }else if(colorGradient == 2){
                  GP = new GradientPaint(0,0, Color.GREEN, w, h, Color.WHITE);
            }else if(colorGradient == 3){
                  GP = new GradientPaint(0,0, Color.YELLOW, w, h, Color.WHITE);
            }
      }
}