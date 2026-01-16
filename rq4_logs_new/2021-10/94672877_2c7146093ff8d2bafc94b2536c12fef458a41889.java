package com.kuldeep.algorithims;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * @author <a href="kuldeepyadav7291@gmail.com">Kuldeep</a>
 */
public class SudokoSolver {
    private boolean isValidPosition(int row, int col, int value, List<ArrayList<Integer>> board) {
        if (board.get(row).contains(value)) return false;

        Optional<Integer> possiblyInCol = board.stream()
                                               .map(r -> r.get(col))
                                               .filter(val -> val.equals(value))
                                               .findAny();
        if (possiblyInCol.isPresent()) return false;

        int rowOffset = row / 3;
        int colOffset = col / 3;

        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                int rowIdx = rowOffset * 3 + i;
                int colIdx = colOffset * 3 + j;

                if (board.get(rowIdx).get(colIdx).equals(value)) return false;
            }
        }

        return true;

    }

    private boolean sodukoHelper(int row, int col, List<ArrayList<Integer>> board) {
        int currentRow = row;
        int currentCol = col;

        if (currentCol >= board.get(currentRow).size()) {
            currentCol = 0;
            currentRow += 1;

            if (currentRow >= board.size()) return true;
        }

        int existingItem = board.get(currentRow).get(currentCol);

        if (existingItem != 0) return sodukoHelper(currentRow, currentCol + 1, board);

        for (int num = 1; num <= 9; num++) {
            if (isValidPosition(currentRow, currentCol, num, board)) {
                board.get(currentRow).set(currentCol, num);

                if (sodukoHelper(currentRow, currentCol + 1, board)) return true;
            }
        }

        board.get(currentRow).set(currentCol, 0);
        return false;
    }

    public List<ArrayList<Integer>> solveSudoku(List<ArrayList<Integer>> board) {
        sodukoHelper(0, 0, board);
        return board;
    }
}