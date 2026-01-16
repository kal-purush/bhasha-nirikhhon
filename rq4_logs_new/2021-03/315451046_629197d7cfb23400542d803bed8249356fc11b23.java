package com.example.pacmanlike.objects;

import android.graphics.Canvas;
import android.graphics.Paint;

import com.example.pacmanlike.gamemap.GameMap;
import com.example.pacmanlike.gamemap.tiles.Tile;
import com.example.pacmanlike.main.AppConstants;
import com.example.pacmanlike.objects.ghosts.Ghost;

public class Pells {

    int _numberOfPells;
    private static int _PELLSCORE = 20, _POWERPELLSCORE = 50;

    public int getNumberOfPells() {return _numberOfPells; }

    public Pells(GameMap map) {
        _numberOfPells = 0;

        for (Vector powerPell :map.getPowerPelletsPosition()) {

            int x = powerPell.x;
            int y = powerPell.y;

            Tile tile = map.getTile(x,y);

            if(!tile.type.equals("Empty") && !tile.type.equals("Home") &&
                    !(x == map.getStartingPacPosition().x && y == map.getStartingPacPosition().y)){
                tile.setFood(Food.PowerPellet);
                _numberOfPells++;
            }
        }



        for (int y = 0; y < AppConstants.MAP_SIZE_Y; y++) {
            for (int x = 0; x < AppConstants.MAP_SIZE_X; x++)
            {
                Tile tile = map.getTile(x,y);

                if(!tile.type.equals("Empty") && !tile.type.equals("Home") && tile.getFood() != Food.PowerPellet &&
                        !(x == map.getStartingPacPosition().x && y == map.getStartingPacPosition().y)) {
                    tile.setFood(Food.Pellet);
                    _numberOfPells++;
                }
            }
        }
    }

    public int update(){
        GameMap gameMap = AppConstants.getGameMap();
        Vector position = AppConstants.getEngine().getPacman().getAbsolutePosition();

        if(AppConstants.testCenterTile(position)) {
            Tile tile = gameMap.getAbsoluteTile(position.x, position.y);

            if(tile.getFood() == Food.Pellet) {

                tile.setFood(Food.None);
                _numberOfPells--;
                return  _PELLSCORE;

            } else if(tile.getFood() == Food.PowerPellet) {

                AppConstants.getEngine().getGhostsEngine().startVulnereble();
                tile.setFood(Food.None);
                _numberOfPells--;
                return   _POWERPELLSCORE;
            }
        }
        return 0;
    }

    public void draw(Canvas canvas, Paint _paintPells, Paint _paintPowerPells) {

        GameMap map = AppConstants.getGameMap();
        int _blockSize = AppConstants.getBlockSize();

        for (int y = 0; y < AppConstants.MAP_SIZE_Y; y++) {
            for (int x = 0; x < AppConstants.MAP_SIZE_X; x++)
            {
                if(map.getTile(x,y).getFood() == Food.Pellet) {
                    canvas.drawCircle(x * _blockSize + _blockSize / 2, y * _blockSize + _blockSize / 2, AppConstants.getBlockSize() * 0.1f, _paintPells);
                } else if(map.getTile(x,y).getFood() == Food.PowerPellet) {

                    canvas.drawCircle(x * _blockSize + _blockSize / 2, y * _blockSize + _blockSize / 2, AppConstants.getBlockSize() * 0.2f, _paintPells);
                    canvas.drawCircle(x * _blockSize + _blockSize / 2, y * _blockSize + _blockSize / 2, AppConstants.getBlockSize() * 0.1f, _paintPowerPells);
                }
            }
        }
    }
}