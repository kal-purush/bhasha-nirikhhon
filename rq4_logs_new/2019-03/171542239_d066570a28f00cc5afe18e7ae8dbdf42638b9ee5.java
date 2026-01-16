/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package videogame;

import java.awt.image.BufferedImage;

/**
 *
 * @author charles
 */
public class Animation {
    private int speed;
    private int index;
    private long lastTime;
    private long timer;
    
    private boolean done;
    
    private BufferedImage[] frames;
    
    public Animation(BufferedImage[] frames, int speed) {
        this.frames = frames;
        this.speed = speed;
        
        index = 0;
        timer = 0;
        lastTime = System.currentTimeMillis();
        done = false;
    }
    
    public BufferedImage getCurrentFrame() {
        return frames[index];
    }
    
    public void tick() {
        timer += System.currentTimeMillis() - lastTime;
        lastTime = System.currentTimeMillis();
        
        if (timer > speed) {
            index++;
            timer = 0;
            
            if (index >= frames.length) {
                done = true;
                index = 0;
            }
        }
    }

    public void setDone(boolean done) {
        this.done = done;
    }

    public boolean isDone() {
        return done;
    }
}