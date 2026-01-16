package com.chess.Chess;

import java.util.ArrayList;

public class Board {
    Figure[][] position = new Figure[8][8];

    String player1;
    String player2;

    public Board(){
        this.position[0][0] = new Rook(0, 0, false, false);
        this.position[0][1] = new Rook(0, 1, false, false);
        this.position[0][2] = new Rook(0, 2, false, false);
        this.position[0][3] = new Rook(0, 3, false, false);
        this.position[0][4] = new Rook(0, 4, false, false);
        this.position[0][5] = new Rook(0, 5, false, false);
        this.position[0][6] = new Rook(0, 6, false, false);
        this.position[0][7] = new Rook(0, 7, false, false);

        this.position[7][0] = new Rook(7, 0, true, false);
        this.position[7][1] = new Rook(7, 1, true, false);
        this.position[7][2] = new Rook(7, 2, true, false);
        this.position[7][3] = new Rook(7, 3, true, false);
        this.position[7][4] = new Rook(7, 4, true, false);
        this.position[7][5] = new Rook(7, 5, true, false);
        this.position[7][6] = new Rook(7, 6, true, false);
        this.position[7][7] = new Rook(7, 7, true, false);
    }

    public Figure getFigure(int row, int column){
        return position[row][column];
    }

    public void Move(int figureRow, int figureColumn, int posRow, int posColumn){
        try {
            position[figureRow][figureColumn].move(position, posRow, posColumn);
        }
        catch(Exception e) {
            //  Block of code to handle errors
        }
    }
    public Figure[][] getFiguresOnBoard(){
        return position;
    }

}