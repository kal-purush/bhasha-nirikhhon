package com.androidgame.jingfu.turtlegame;

import com.androidgame.jingfu.fjturtle.framework.Game;
import com.androidgame.jingfu.fjturtle.framework.Graphics;

import java.util.Random;

/**
 * Created by jing on 2016/7/1.
 */
public class Fruit {

    private int x,y;//the position info of the fruit
    private boolean live = true;// live or dead.

    private static Random r = new Random();
    private Brightness bn = Brightness.H;
    public GameScreen gameScreen;


    public Fruit (int x, int y) {
        this.x = x;
        this.y = y;
        initializeFruit();
    }//constructor of fruit. given x&y can new a fruit.

    public Fruit (GameScreen screen) {
        this.gameScreen = screen;
        initializeFruit();
    }

    public void draw(Graphics g) {

        if (!live) {
            initializeFruit();
            this.live = true;
        }//if the fruit has been eaten, find a new location and draw a fruit.

        for(int i = 0; i < gameScreen.leds.size();i++){
            if(gameScreen.leds.get(i).getX() == x && gameScreen.leds.get(i).getY() == y){
                gameScreen.leds.get(i).draw(g,bn);
            }
        }

    }


    public boolean hitSnake(Snake s) {

        for (int i = 0; i < 3; i++){
            // if position of fruit is equal to the any part of snake's positon, then the fruit hits snake
            if(this.x == s.x.get(i) && this.y == s.y.get(i)) {
                return true;
            }
        }
        return false;
    }

    public void initializeFruit() {
        // randmonly generate a position for a fruit when it's be created.
        // and make sure the position does not collide with existing snake and inside the screen
        do {
            this.x = r.nextInt(8)+1;/*inside the screen: x belongs to (1,9)*/
            this.y = r.nextInt(8)+1;/*inside the screen: y belongs to (1,9)*/
        } while(Math.abs(this.x-this.y) >= 5 || hitSnake(gameScreen.mySnake));
        /*inside the screen: distance btw x and y is smaller than 5*/  /*does not collide with existing snake*/
    }

    public int getX() {
        return x;
    }

    public int getY() {
        return y;
    }

    public boolean getLive() {
        return live;
    }


    public void setLive(boolean live) {
        this.live = live;
    }

}