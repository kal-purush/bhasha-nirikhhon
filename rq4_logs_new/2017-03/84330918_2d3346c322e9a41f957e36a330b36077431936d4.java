/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ponginvader;

import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Rectangle;
import java.awt.geom.AffineTransform;
import static ponginvader.Commons.DEFENDER_HEIGHT;
import static ponginvader.Commons.DEFENDER_WIDTH;

/**
 *
 * @author mariobsdd
 */
public class Cannon implements Commons {
    private PongInvader game;
    private int left,right;
    private int x,y;
    private int angulo =0;
    boolean rotate;

    public Cannon(PongInvader game, int left, int right, int x, int y,boolean rotate) {
        this.game = game;
        this.left = left;
        this.right = right;
        this.x = x;
        this.y = y;
        this.rotate = rotate;
    }
    public void update(){
//        if((x >= 300) && (x<game.getWidth()-300-DEFENDER_WIDTH)){
//            y=100;
//            x=300+DEFENDER_WIDTH/2;
//        }
       
    }
    
    public void pressed(int keyCode){
        if(keyCode == left && angulo>-60){
             angulo -=10;
        }
        else if(keyCode == right && angulo<50){
            angulo += 10;
        } 
    }
    
    public void released(int keyCode) {
        if (keyCode == left || keyCode == right)
            angulo+=0;
    }

    public Rectangle getBounds() {
        return new Rectangle(x, y, SHOOTER_WIDTH, SHOOTER_HEIGHT);
    }

    public void paint(Graphics g) {
        Graphics2D g2 = (Graphics2D)g.create();
        Rectangle rect = new Rectangle(0,0, SHOOTER_WIDTH, SHOOTER_HEIGHT);
        if(!rotate){//arriba
            g2.rotate(-Math.toRadians((double)angulo),x,y);
            //System.out.println("angulo: "+angulo);
            g2.translate(x,y);
            g2.draw(rect);
            g2.fill(rect);
            g2.dispose();    
        }
        else{//abajo
            g2.rotate(Math.toRadians(180), x, y);
            g2.rotate(-Math.toRadians((double)angulo),x,y);
            //System.out.println("angulo: "+angulo);
            g2.translate(x,y);
            g2.draw(rect);
            g2.fill(rect);
            g2.dispose();  
        }
        
    }
    
}