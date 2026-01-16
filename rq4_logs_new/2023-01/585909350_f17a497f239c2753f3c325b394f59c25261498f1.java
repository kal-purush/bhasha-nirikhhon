package com.serenebond.entities;

import com.serenebond.graphics.Spritesheet;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.util.ArrayList;
import java.util.List;

public class PlayerHairstyle {
    private static final List<BufferedImage[]> hairStyles = new ArrayList<>();

    // todo Some sort of global resource loader may make sense here
    private static final Spritesheet spritesheet = new Spritesheet("/hairStyles.png");

    static {
        for (int y = 0; y < 8; y++) {
            for (int x = 0; x < 2; x++) {
                BufferedImage[] hairStyle = new BufferedImage[4];
                for (int dir = 0; dir < 4; dir++) {
                    hairStyle[dir] = spritesheet.getSprite(x * 64 + (dir * 16), y * 16, 16, 16);
                }
                hairStyles.add(hairStyle);
            }
        }
    }

    private int hairStyle;
    private Color hairColor = Color.CYAN;

    public PlayerHairstyle(int hairStyle, int hairColor) {
        this.hairStyle = hairStyle;
        setHairColor(hairColor);
    }

    public BufferedImage getHairStyle(int direction) {
        return hairStyles.get(hairStyle)[direction];
    }

    public int getHairStyle() {
        return hairStyle;
    }

    public void setHairStyle(int hairStyle) {
        this.hairStyle = hairStyle;
    }

    public Color getHairColor() {
        return hairColor;
    }

    public void setHairColor(int hairColor) {
        this.hairColor = new Color((hairColor >> 16) & 0xFF, (hairColor >> 8) & 0xFF, hairColor & 0xFF);
    }
}