import javax.swing.*;
import java.awt.*;

/**
 * Class Canvas - a class to allow for simple graphical 
 * drawing on a canvas.
 * 
 * @author: Bruce Quig
 * @author: Michael Kolling (mik)
 *
 * @version: 1.7
 * date:     27.04.2006
 *	
 * changes:
 *   19.11.1999   mik     added proper buffering and screen update
 *   10.08.2000   mik     modified "setVisible" to bring window to front
 *   06.10.2000   mik     fixed a bug in "setSize"
 *   24.05.2001   mik     added "erase" method
 *   26.04.2006	  uh	  adapted to fit the needs of the Maze application
 *   19.04.2008   uh      adapted to fit the needs of the Fractals application
 */

public class Canvas extends JPanel
{
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private JFrame frame;
    private Graphics2D graphic = null;
    private Color backgroundColour;
    private Image canvasImage;
    private boolean trace = false;	// UH 2008-04-19
  
    /**
     * Create a Canvas.
     * @param frame  the frame that will contain this canvas
     * @param width  the desired width for the canvas
     * @param height  the desired height for the canvas
     * @param bgClour  the desired background colour of the canvas
     */
    public Canvas(JFrame frame, Color bgColour)
    {
    	this.frame = frame;
//        setPreferredSize(new Dimension(width, height));
        backgroundColour = bgColour;
    }

    /**
     * Set the canvas visibility and brings canvas to the front of screen
     * when made visible. This method can also be used to bring an already
     * visible canvas to the front of other windows.
     * @param visible  boolean value representing the desired visibility of
     * the canvas (true or false) 
     */
    public void setVisible(boolean visible)
    {
        if(graphic == null) {
            // first time: instantiate the offscreen image and fill it with
            // the background colour
            Dimension size = getSize();
            canvasImage = createImage(size.width, size.height);
            graphic = (Graphics2D)canvasImage.getGraphics();
            graphic.setColor(backgroundColour);
            graphic.fillRect(0, 0, size.width, size.height);
            graphic.setColor(Color.blue);
        }
       // frame.show();
    }

    /**
     * Provide information on visibility of the Canvas.
     * @return  true if canvas is visible, false otherwise
     */
    public boolean isVisible()
    {
        return frame.isVisible();
    }

    /**
     * Draw the outline of a given shape onto the canvas.
     * @param  shape  the shape object to be drawn on the canvas
     */
    public void draw(Shape shape)
    {
        graphic.draw(shape);
        repaint();
    }
 
    /**
     * Fill the internal dimensions of a given shape with the current 
     * foreground colour of the canvas.
     * @param  shape  the shape object to be filled 
     */
    public void fill(Shape shape)
    {
        graphic.fill(shape);
        repaint();
    }

    /**
     * Erase the whole canvas.
     */
    public void erase()
    {
        Color original = graphic.getColor();
        graphic.setColor(backgroundColour);
        Dimension size = getSize();
        graphic.fill(new Rectangle(0, 0, size.width, size.height));
        graphic.setColor(original);
        repaint();
    }

    /**
     * Erase a given shape's interior on the screen.
     * @param  shape  the shape object to be erased 
     */
    public void erase(Shape shape)
    {
        Color original = graphic.getColor();
        graphic.setColor(backgroundColour);
        graphic.fill(shape);              // erase by filling background colour
        graphic.setColor(original);
        repaint();
    }

    /**
     * Erases a given shape's outline on the screen.
     * @param  shape  the shape object to be erased 
     */
    public void eraseOutline(Shape shape)
    {
        Color original = graphic.getColor();
        graphic.setColor(backgroundColour);
        graphic.draw(shape);  // erase by drawing background colour
        graphic.setColor(original);
        repaint();
    }

    /**
     * Draws an image onto the canvas.
     * @param  image   the Image object to be displayed 
     * @param  x       x co-ordinate for Image placement 
     * @param  y       y co-ordinate for Image placement 
     * @return  returns boolean value representing whether the image was 
     *          completely loaded 
     */
    public boolean drawImage(Image image, int x, int y)
    {
        boolean result = graphic.drawImage(image, x, y, null);
        repaint();
        return result;
    }

    /**
     * Draws a String on the Canvas.
     * @param  text   the String to be displayed 
     * @param  x      x co-ordinate for text placement 
     * @param  y      y co-ordinate for text placement
     */
    public void drawString(String text, int x, int y)
    {
        graphic.drawString(text, x, y);   
        repaint();
    }

    /**
     * Erases a String on the Canvas.
     * @param  text     the String to be displayed 
     * @param  x        x co-ordinate for text placement 
     * @param  y        y co-ordinate for text placement
     */
    public void eraseString(String text, int x, int y)
    {
        Color original = graphic.getColor();
        graphic.setColor(backgroundColour);
        graphic.drawString(text, x, y);   
        graphic.setColor(original);
        repaint();
    }

    /**
     * Draws a line on the Canvas.
     * @param  x1   x co-ordinate of start of line 
     * @param  y1   y co-ordinate of start of line 
     * @param  x2   x co-ordinate of end of line 
     * @param  y2   y co-ordinate of end of line 
     */
    public void drawLine(int x1, int y1, int x2, int y2)
    {
        graphic.drawLine(x1, y1, x2, y2);  
        repaint();
    }

    /**
     * Sets the foreground colour of the Canvas.
     * @param  newColour   the new colour for the foreground of the Canvas 
     */
    public void setForegroundColour(Color newColour)
    {
        graphic.setColor(newColour);
    }

    /**
     * Returns the current colour of the foreground.
     * @return   the colour of the foreground of the Canvas 
     */
    public Color getForegroundColour()
    {
        return graphic.getColor();
    }

    /**
     * Sets the background colour of the Canvas.
     * @param  newColour   the new colour for the background of the Canvas 
     */
    public void setBackgroundColour(Color newColour)
    {
        backgroundColour = newColour;   
        graphic.setBackground(newColour);
    }

    /**
     * Returns the current colour of the background
     * @return   the colour of the background of the Canvas 
     */
    public Color getBackgroundColour()
    {
        return backgroundColour;
    }

    /**
     * Sets the size of the canvas.
     * @param  width    new width 
     * @param  height   new height 
     */
    public void setSize(int width, int height)
    {
        setPreferredSize(new Dimension(width, height));
        Image oldImage = canvasImage;
        canvasImage = createImage(width, height);
        graphic = (Graphics2D)canvasImage.getGraphics();
        graphic.drawImage(oldImage, 0, 0, null);
        frame.pack();
    }
    
    /**
     * Waits for a specified number of milliseconds before finishing.
     * This provides an easy way to specify a small delay which can be
     * used when producing animations.
     * @param  milliseconds  the number 
     */
    public void wait(int milliseconds)
    {
        try
        {
            Thread.sleep(milliseconds);
        } 
        catch (InterruptedException e)
        {
            // ignoring exception at the moment
        }
    }

    public void paint(Graphics g) {
    	g.drawImage(canvasImage, 0, 0, null);
    }
    
    // UH 2008-04-19
    public void toggleTrace() {
    	trace = ! trace;
    }
    
    //  UH 2008-04-19
    public void repaint() {
    	if ( trace ) 
            // This allows you to view the drawing process step by step
            paintImmediately(new Rectangle(getSize()));
    	else
    		super.repaint();
    }
}
