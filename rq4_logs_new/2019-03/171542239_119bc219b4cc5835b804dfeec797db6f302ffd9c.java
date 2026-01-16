/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package videogame;

import java.awt.Graphics;
import java.awt.image.BufferedImage;

/**
 *
 * @author charles
 */
public class PowerUp extends Item {
    
    private int speed;
    private BufferedImage image;

    public PowerUp(int x, int y, int width, int height, BufferedImage image) {
        super(x, y, width, height);
        this.image = image;
        speed = 5;
    }

    @Override
    public void tick() {
        setY(getY() + speed);
    }

    @Override
    public void render(Graphics g) {
        g.drawImage(image, getX(), getY(), getWidth(), getHeight(), null);
    }
    
}