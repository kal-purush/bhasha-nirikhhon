/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package background;

import java.awt.Color;
import java.awt.Graphics;
import java.awt.Graphics2D;

/**
 *
 * @author ΙΩΑΝΝΑ
 */
public class Background {

    
    public void backgroundColour(int width , int height , int rows , int cols , Graphics2D g){
    
        int i;

        // draw the rows
        int rowHt = height / (rows);
        for (i = 0; i < rows; i++) {
            g.drawLine(0, i * rowHt, width, i * rowHt);
        }

        // draw the columns
        int rowWid = width / (cols);
        for (i = 0; i < cols; i++) {
            g.drawLine(i * rowWid, 0, i * rowWid, height);
        }
    }
}