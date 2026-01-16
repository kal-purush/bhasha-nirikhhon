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
public class Collectible extends GameObject{
    private String name;
    private int value; 

    public Collectible(double nowx, double nowy, String sprpath, String itemname) throws SlickException {
        super(nowx, nowy, sprpath, itemname);        
        name = itemname;
    }
    
    
    
}