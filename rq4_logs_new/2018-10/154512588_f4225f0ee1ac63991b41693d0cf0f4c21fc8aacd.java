package com.tilegame.game.handler;

import com.badlogic.gdx.Input;
import com.badlogic.gdx.InputAdapter;
import com.badlogic.gdx.math.Vector2;

/**
 * Created by: Harrison on 26 Oct 2018
 */
public class InputHandler extends InputAdapter {

    /*Entity entity;

    InputHandler(Entity entity, float delta) {
        this.entity = entity;
    }

    @Override
    public boolean keyDown(int keycode) {
        if(keycode == Input.Keys.W){
            entity.move(new Vector2(0, 1));
        }
        if(keycode == Input.Keys.S){
            entity.move(new Vector2(0, -1));
        }
        if(keycode == Input.Keys.A){
            entity.move(new Vector2(-1, 0));
        }
        if(keycode == Input.Keys.D){
            entity.move(new Vector2(1, 0));
        }
        return false;
    }*/

    @Override
    public boolean keyUp(int keycode) {
        return super.keyUp(keycode);
    }

    @Override
    public boolean touchUp(int screenX, int screenY, int pointer, int button) {
        return super.touchUp(screenX, screenY, pointer, button);
    }
}