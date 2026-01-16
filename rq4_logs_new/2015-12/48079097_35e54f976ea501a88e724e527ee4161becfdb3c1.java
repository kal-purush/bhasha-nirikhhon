// @author Colin Wakefield
package gameWithAlex;

import java.awt.image.BufferedImage;

public class Animaton {
	private BufferedImage[] frames;
	private int currentFrame;
	
	private long startTime;
	private long delay;
	
	public Animaton(){startTime = 0;}
	// initaly sets up frame
	public void setFrames( BufferedImage[]images)
	{
		frames = images;
		if(currentFrame >= frames.length) currentFrame = 0;
	}
	// sets delay 
	public void setDelay(long d )
	{
		delay = d;
	}
	// updates frame to what is needed
	public void update()
	{
		if(delay ==-1)return;
		
		long elapsed = (System.nanoTime() - startTime)/1000000;
		if (elapsed > delay)
		{
			currentFrame++;
			startTime = System.nanoTime();
		}
		if(currentFrame >= frames.length) currentFrame = 0;
	}
	public BufferedImage getImage()
	{
		return frames[currentFrame];
	} 

}