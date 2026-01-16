import java.awt.Color;

import graphics.Canvas;

public class Program {
    // Canvas GUI instance providing the drawing area on which to plot the functions
    private static Canvas canvas;

    public static void main(String[] args) {
        
        // Create the canvas instance, set its range to x:[-360, 360] and y:[-240, 240] then 
        // open it on the screen. Use canvas.plot(pX, pY) to plot a point at the (pX, pY) coords
        canvas = new Canvas();
        canvas.setRange(-360, -240, 360, 240);
        canvas.open();

        // Draw a diagonal of red points on the canvas
        canvas.pause();
        canvas.setColor(Color.red);
        for (int i = 0; i < 100; i++) {
            canvas.plot(i, i);
        }

        // Pause and close the canvas then terminate the program.
        canvas.pause();
        canvas.close();
    }
}