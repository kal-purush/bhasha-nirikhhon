package com.serenebond.tile;

import com.serenebond.world.Camera;
import com.serenebond.world.World;

import java.awt.*;

public sealed class FloorTile extends TexturedTile permits BlendingTile {
    public int order = -1;

    public FloorTile(int u, int v) {
        super(u, v);
    }

    @Override
    public void draw(Graphics graphics, int x, int y, World world) {
        super.draw(graphics, x, y, world);

        for (int z = -1; z <= 1; z++) {
            for (int w = -1; w <= 1; w++) {

                if ((x != 0 || y != 0) && (x + z) > 0 && (x + z) < World.tiles.length - 1 && (y + w) > 0 && (y + w) < World.tiles[0].length - 1) {
                    var tile = World.tiles[x + z][y + w];

                    if (tile != this && tile instanceof BlendingTile blendingTile && blendingTile.order > order) {
                        graphics.drawImage(blendingTile.images[1 - z][1 - w], x * 16 - Camera.x, y * 16 - Camera.y, null);
                    }
                }
            }
        }
    }

    @Override
    public boolean collides() {
        return false;
    }
}