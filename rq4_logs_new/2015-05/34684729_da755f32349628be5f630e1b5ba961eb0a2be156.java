package com.bug.game;

import com.badlogic.gdx.graphics.g2d.Batch;
import com.badlogic.gdx.graphics.g2d.Sprite;
import com.badlogic.gdx.scenes.scene2d.Actor;

import java.util.ArrayList;

/**
 * Created by Lego on 30.05.2015.
 */
public class BLabel extends Actor {
    ArrayList<Sprite> LabelSprites = new ArrayList<Sprite>();

    BLabel (String String) {

        for (int i = 0; i<String.length(); i++) {
            LabelSprites.add(new BTextSymbol(String.charAt(i)).getSprite());
        }

    }

    @Override
    public void draw(Batch batch, float parentAlpha) {
        for(Sprite buff:LabelSprites) {
            buff.draw(batch,parentAlpha);
        }
    }


}