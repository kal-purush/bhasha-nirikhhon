package com.olivierpicard.Grapher.Tools.VisualTools;

import com.olivierpicard.Grapher.Scene;

import java.awt.*;

public class CenterButton extends Button
{

    public CenterButton(IButtonCallback callback) {
        super(callback);
    }


    public CenterButton(IButtonCallback callback, String iconPath) {
        super(callback, iconPath);
    }


    @Override
    public void Draw(Graphics2D g, int x, int y) {
        if(Scene.deltaOriginePosition.get_x() != 0 && Scene.deltaOriginePosition.get_y() != 0)
            super.Draw(g, x, y);
    }


}