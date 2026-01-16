package com.bug.game;

import com.badlogic.gdx.graphics.Texture;
import com.badlogic.gdx.graphics.g2d.Batch;
import com.badlogic.gdx.graphics.g2d.Sprite;
import com.badlogic.gdx.scenes.scene2d.Actor;

/**
 * Created by Lego on 27.05.2015.
 */
public class Buffs extends Actor {
    public String Buffs;
    public Sprite Sprite;
    private Bounds bound;
    private Texture texture;
    public int CurrentPossition;  // Curren CellID
    private boolean WorKing = true;

    public Buffs(String buffs, int CellID, final Bounds bound){
        this.bound = bound;
        this.Buffs = buffs;
        this.CurrentPossition = CellID;

    //    setHeight(bound.getSize());
    //    setWidth(bound.getSize());
        setPosition(bound.getX(), bound.getY());


        texture = new Texture(buffs+".png");
        Sprite = new Sprite(texture);
    }

public void SetBuffs(String buffs){
    this.Buffs = buffs;
}

public String getBuffs(){
    return this.Buffs;
}



    @Override
public void draw(Batch batch, float alpha) {
        if (WorKing) {
            Sprite.setOriginCenter();
            Sprite.setSize(bound.getSize(), bound.getSize());
            Sprite.setPosition(bound.getX(), bound.getY());
            Sprite.draw(batch);
        }

    }

    public void ApplyBuff (Bug bug) {
        if (this.Buffs == "heal") {
            int Durability = bug.getDurability();
            bug.setDurability(Durability+(Durability/100)*15);
        }

        if(this.Buffs == "damage"){
            int damage =bug.getPower();
            bug.setPower(damage +5);
        }

        if(this.Buffs == "speed"){
            bug.CruisingRange+=3;
        }
    }

    public void clear(){
        this.WorKing = false;
    }

}