package com.example.pacmanlike.gamemap;

import android.graphics.Bitmap;

import com.example.pacmanlike.gamemap.tiles.Tile;
import com.example.pacmanlike.objects.Vector;
import com.example.pacmanlike.main.AppConstants;

public class GameMap {
    private Tile[][] _map;
    private Vector _startingPacPosition;

    private Bitmap _background;

    public void setStartingPacPosition(Vector position) { _startingPacPosition = position; }
    public Vector getStartingPacPosition() { return _startingPacPosition; }

    public Tile[][] getMap() { return _map; }

    public void setMap(Tile[][] map) {_map = map;}


    public Tile getTile(int x, int y) {return  _map[y][x]; }

    public Tile getAbsoluteTile(int x, int y) {
        int tileX = x / AppConstants.getBlockSize();
        int tileY = y / AppConstants.getBlockSize();

        return getTile(tileX, tileY);
    }

    public void setBackground(Bitmap bitmap) {_background = bitmap; }
    public Bitmap getBackground() {return _background; }

}