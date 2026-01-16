package com.chess.Chess;

public class Figure {
    private int row, column;
    private boolean is_white;
    private String name;

    public int[] return_coordinates() {
        return new int[] { this.row, this.column };
    }

    public void set_coordinates(int row, int column) {
        this.row = row;
        this.column = column;
    }

    public String get_name() {
        return this.name;
    }

    public void set_name(String name) {
        this.name = name;
    }

    public boolean is_white() {
        return this.is_white;
    }

    public void set_is_white(boolean is_white) {
        this.is_white = is_white;
    }

    public void move(Figure[][] curr_position, int new_row, int new_column) {
    }

    public void get_possible_moves(Figure[][] curr_position) {
    }
}

class ImpossibleMove extends Exception {
    public ImpossibleMove() {
        super("You can`t make this move");
    }
}

class IncorrectPositionOfFigure extends Exception {
    public IncorrectPositionOfFigure() {
        super("You can`t place figure here");
    }
}