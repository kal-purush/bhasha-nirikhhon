package com.tlyong1992.client.model;

import com.tlyong1992.client.view.MainView;

import java.awt.*;

/**
 * USER：tangly
 * DATE：2017/5/27
 * TIME：16:10
 */
public class Explore {
    private int positionX;
    private int positionY;

    private int step = 0;
    private boolean live = true;

    public boolean isLive() {
        return live;
    }

    private int[] sizeArray = {5,10,15,20,30,15,5};

    public Explore(Bullet bullet){
        this.positionX = bullet.getPositionX();
        this.positionY = bullet.getPositionY();
    }

    public void draw(Graphics g, MainView mainView) {
        Color c = g.getColor();
        g.setColor(Color.ORANGE);
        if(step==sizeArray.length){
            step = 0 ;
            live = false;
        }

//      for(;step < sizeArray.length;step++){
        g.fillOval(positionX, positionY, sizeArray[step], sizeArray[step]); //爆炸是一个圆
        step ++;
//      }
        g.setColor(c);
    }

}