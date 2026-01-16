
package pfinalp1_cespinal_aieong;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JTextArea;

public class Enemy extends Thread{
    private int posx;
    private int posy;
    private int points;
    private int posxplayer;
    private char sprite;
    public boolean alive;
    
    
    public Enemy() {
    }
    
    

    public Enemy(int posx, int posy, int point, int pxplay, char icon) {
        this.posx = posx;
        this.posy = posy;
        this.points = point;
        this.posxplayer = pxplay;
        this.sprite = icon;
        this.alive = true;
        
    }
    
    public void moveRight(){
        posy += 1;
    }
    
    public void moveLeft(){
        posy -= 1;
    }
    
    public void moveDown(){
        posx += 1;
    }

    public int getPosx() {
        return posx;
    }

    public void setPosx(int posx) {
        this.posx = posx;
    }

    public int getPosy() {
        return posy;
    }

    public void setPosy(int posy) {
        this.posy = posy;
    }

    public int getPoints() {
        return points;
    }

    public void setPoints(int points) {
        this.points = points;
    }

    public int getPosxplayer() {
        return posxplayer;
    }

    public void setPosxplayer(int posxplayer) {
        this.posxplayer = posxplayer;
    }

    public char getSprite() {
        return sprite;
    }

    public void setSprite(char sprite) {
        this.sprite = sprite;
    }
    

    
    @Override
    public void run(){
        
        while(alive){
            if (posx % 2 == 0) {
                posy++;
                if (posy == 10) {
                    posx++;
                }
            }
            else if (posx % 2 == 0) {
                posy--;
                if (posy == 0) {
                    posx++;
                }
            }
            if(posx == 10){
                alive = false;
            }
        }
        
    }
    
    
    
}