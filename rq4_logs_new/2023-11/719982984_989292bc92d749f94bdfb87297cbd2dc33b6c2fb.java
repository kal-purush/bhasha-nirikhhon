package com.chess.Chess;

import java.util.ArrayList;

public class Bishop extends Figure {
    public Bishop(int row, int column, boolean isWhite) {
        this.set_coordinates(row, column);
        this.set_is_white(isWhite);
        this.set_name("Bishop");
        this.set_not_moved(true);
    }

    private boolean checkDiagonalMove(Figure[][] currPosition, int newRow, int newCol) {
        int currRow = this.return_coordinates()[0];
        int currCol = this.return_coordinates()[1];
        boolean color = this.is_white();

        int rowDirection = Integer.compare(newRow, currRow); // Equals 1 when piece moves up, -1 when down, used as a multiplier
        int colDirection = Integer.compare(newCol, currCol); // Equals 1 when piece moves right, -1 when left, used as a multiplier

        for (int i = 1; i < Math.abs(newRow - currRow); i++) {
            if (currPosition[currRow + i*rowDirection][currCol + i*colDirection] != null) {
                return false;
            }
        }
        return currPosition[newRow][newCol] == null || color != currPosition[newRow][newCol].is_white();
    }

    @Override
    public boolean check_if_move_possible(Figure[][] currPosition, int newRow, int newCol) {
        int currRow = this.return_coordinates()[0];
        int currCol = this.return_coordinates()[1];

        if (Math.abs(newRow - currRow) != Math.abs(newCol - currCol)) { // Any but diagonal moves
            return false;
        }
        return checkDiagonalMove(currPosition, newRow, newCol);
    }

    @Override
    public void move(Figure[][] currPosition, int newRow, int newCol) {
        if (check_if_move_possible(currPosition, newRow, newCol)) {
            int currRow = this.return_coordinates()[0];
            int currCol = this.return_coordinates()[1];
            currPosition[newRow][newCol] = this;
            currPosition[currRow][currCol] = null;
            currPosition[newRow][newCol].set_coordinates(newRow, newCol);
        }
    }

    @Override
    public ArrayList<int[]> get_possible_moves(Figure[][] currPos) {
        ArrayList<int[]> possibleMoves = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            for (int j = 0; j < 8; j++) {
                if (this.check_if_move_possible(currPos, i, j)) {
                    possibleMoves.add(new int[] {i, j});
                }
            }
        }
        return possibleMoves;
    }
}