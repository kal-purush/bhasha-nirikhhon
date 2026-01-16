package com.example.pacmanlike.gamemap;

import com.example.pacmanlike.gamemap.tiles.CrossroadTile;
import com.example.pacmanlike.gamemap.tiles.EmptyTile;
import com.example.pacmanlike.gamemap.tiles.HalfcrossroadTile;
import com.example.pacmanlike.gamemap.tiles.HomeTile;
import com.example.pacmanlike.gamemap.tiles.LeftTeleportTile;
import com.example.pacmanlike.gamemap.tiles.RightTeleportTile;
import com.example.pacmanlike.gamemap.tiles.StraightTile;
import com.example.pacmanlike.gamemap.tiles.Tile;
import com.example.pacmanlike.gamemap.tiles.TurnTile;

public class TileFactory {

    public static Tile createTile(char tileName, String argument) throws Exception {
        Tile tile;

        switch (tileName){
            case 'S' : tile = new StraightTile(argument); break;
            case 'T' : tile = new TurnTile(argument); break;
            case 'H' : tile = new HalfcrossroadTile(argument); break;
            case 'C' : tile = new CrossroadTile(); break;
            case 'R' : tile = new RightTeleportTile(); break;
            case 'L' : tile = new LeftTeleportTile(); break;
            case 'X' : tile = new EmptyTile(); break;
            case 'A' : tile = new HomeTile(argument); break;
            default: throw new Exception("Wrong tile format.");
        }
        return tile;
    }

    public static Tile createTile(String tileName) throws Exception{
        return createTile(tileName.charAt(0),tileName.substring(1));
    }
}