package com.androidgame.jingfu.turtlegame;

import android.graphics.Color;
import android.graphics.Paint;

import com.androidgame.jingfu.fjturtle.framework.Graphics;

/**
 * Created by handsomemark on 6/30/16.
 */
public class Led {

    public enum Brightness {
        H,M,L
    }

    private int cx,cy;
    private int gameX, gameY;
    public static final int RADIUS= Integer.parseInt(PropertyManager.getProperty("ledRadius"));
    public static final int BOUNDTHICK= Integer.parseInt(PropertyManager.getProperty("ledBoundThick"));
    private Brightness b;

    public Led(int cx ,int cy, int gameX, int gameY) {
        this.cx = cx;
        this.cy = cy;
        this.gameX = gameX;
        this.gameY = gameY;
    }

    public void draw(Graphics g) {//draw the outline of a led
        fillCircle(cx,cy,RADIUS,Color.RED,g);
        fillCircle(cx,cy,RADIUS-(BOUNDTHICK)/2,Color.BLACK,g);
    }

    public void draw(Graphics g, Brightness b) {//draw a led on graphcis g based on brightness b
        int color = 0;
        Color c = new Color();
        switch (b) {//set the color for this led based on its brightness
            case H: color = c.rgb(255,18,18); break;
            case M: color = c.rgb(255,58,58);break;
            case L: color = c.rgb(255,107,107);break;
        }
        fillCircle(cx,cy,RADIUS-(BOUNDTHICK)/2,color,g);
    }

    private void fillCircle(int cx, int cy, int radius, int color, Graphics g) {
        Paint p = new Paint();
        p.setColor(color);
        p.setStyle(Paint.Style.FILL);
        g.drawCircle(cx,cy,radius,p);
    }
}