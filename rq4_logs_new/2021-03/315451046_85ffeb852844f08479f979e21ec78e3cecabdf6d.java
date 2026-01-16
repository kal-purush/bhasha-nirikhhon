package com.example.pacmanlike;

public class HomeTile extends Tile {
    int number;

    public HomeTile(String number) {
        super(0);
        type = "Home";
        this.number = Integer.parseInt(number);
        fillMoves();
    }

    private void fillMoves(){
        switch (number){
            case 1: possibleMoves.add(new Vector(1,0));
            case 2: possibleMoves.add(new Vector(0,-1));
            case 3: possibleMoves.add(new Vector(-1,0));
        }
    }

    @Override
    public String toString() {
        return "A" + number;
    }
}