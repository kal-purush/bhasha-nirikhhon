package com.example.pacmanlike.gamemap.tiles;

import com.example.pacmanlike.R;
import com.example.pacmanlike.objects.Direction;

public class DoorTile extends Tile {
    public DoorTile(){
        super(0);
        type = "Door";
        drawableId = R.drawable.door;
        fillMoves();
    }

    private void fillMoves(){
        switch (rotation){
            case 0:
                possibleMoves.add(Direction.LEFT);
                possibleMoves.add(Direction.RIGHT);
                break;
            case 90:
                possibleMoves.add(Direction.UP);
                possibleMoves.add(Direction.DOWN);
                break;
        }
    }

    @Override
    public String toString() {
        return "D" + rotation;
    }
}