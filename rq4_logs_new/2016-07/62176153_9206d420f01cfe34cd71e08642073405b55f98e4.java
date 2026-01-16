package com.androidgame.jingfu.turtlegame;

import android.graphics.Color;
import android.graphics.Paint;
import android.util.Log;

import com.androidgame.jingfu.fjturtle.framework.FileIO;
import com.androidgame.jingfu.fjturtle.framework.Game;
import com.androidgame.jingfu.fjturtle.framework.Graphics;
import com.androidgame.jingfu.fjturtle.framework.Input.TouchEvent;
import com.androidgame.jingfu.fjturtle.framework.Screen;
import com.androidgame.jingfu.fjturtle.framework.implementation.AndroidGame;

import java.util.List;

/**
 * Created by jing on 2016/7/2.
 */
public class RankScreen extends Screen {
    FileIO fileIO ;
    private int flag = -1;
    private int first,second,third;
    Paint paint;

    public RankScreen(Game game) {
        super(game);
        Assets.rankScreen = game.getGraphics().newImage("rankscreen.png", Graphics.ImageFormat.ARGB8888);
        this.fileIO = game.getFileIO();
        this.first = fileIO.getIntegerFromPref("1st", -1);
        this.second = fileIO.getIntegerFromPref("2nd", -2);
        this.third = fileIO.getIntegerFromPref("3rd", -3);
        this.paint = new Paint();
        paint.setTextSize(80);
        paint.setTextAlign(Paint.Align.CENTER);
        paint.setAntiAlias(true);
        paint.setColor(Color.WHITE);
    }
    @Override
    public void update(float deltaTime) {
        if (first >= 0 && second >= 0 && third >= 0) {
            flag = 3; //rank has 3 scores.
        } else if (first >= 0 && second >= 0){
            flag = 2;
        } else if (first >= 0) {
            flag = 1;
        } else {  flag = 0; }
    }

    @Override
    public void paint(float deltaTime) {
        Graphics g = game.getGraphics();
        g.drawScaledImage(Assets.rankScreen, 0, 0, ((AndroidGame) game).landscapeWidth, ((AndroidGame) game).landscapeHeight, 0, 0, Assets.rankScreen.getWidth(), Assets.rankScreen.getHeight());
        switch (flag) {
            case 3: // draw three scores : 3,2,1
                g.drawString(""+third,((AndroidGame)game).landscapeWidth*815/(Assets.rankScreen.getWidth()) ,((AndroidGame)game).landscapeHeight*720/(Assets.rankScreen.getHeight()),paint);
            case 2: // draw two scores : 2,1
                g.drawString(""+second,((AndroidGame)game).landscapeWidth*790/(Assets.rankScreen.getWidth()) ,((AndroidGame)game).landscapeHeight*615/(Assets.rankScreen.getHeight()),paint);
            case 1: // only draw one score: 1
                g.drawString(""+first,((AndroidGame)game).landscapeWidth*820/(Assets.rankScreen.getWidth()) ,((AndroidGame)game).landscapeHeight*508/(Assets.rankScreen.getHeight()),paint);
        }
    }

    @Override
    public void pause() {

    }

    @Override
    public void resume() {

    }

    @Override
    public void dispose() {

    }

    @Override
    public void backButton() {
        game.setScreen(new MenuScreen(game));
    }
}