/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bitzgame.bitz;

import org.newdawn.slick.SlickException;

/**
 *
 * @author Timo
 */
public class Enemy extends GameObject {
    
    private int health;
    
    public Enemy(float nowx, float nowy, String sprpath) throws SlickException {
        super(nowx, nowy, sprpath);
        health = 2;
    }
    
    public int getHealth(){
        return this.health;
    }
    
    public void setHealth(int i){
        this.health = i;
    }
    
    public void addToHealth(int i){
        this.health += i;
    }
    
}