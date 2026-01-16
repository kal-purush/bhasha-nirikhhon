/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bitzgame.bitz;

/**
 *
 * @author Timo
 */
public class Camera {
    private float x;
    private float y;
    
    public Camera(float nowX, float nowY){
        this.x = nowX;
        this.y = nowY;
    }
    
    public float getX(){
        return this.x;
    }
    
    public void setX(float nowX){
        this.x = nowX;
    }
    
    public float getY(){
        return this.y;
    }
    
    public void setY(float nowY){
        this.y = nowY;
    }
    
}