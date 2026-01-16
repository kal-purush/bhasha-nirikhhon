package com.coding.challenge.backtracking;

import java.util.Arrays;

class SudokuSolver {
	private static final char ZERO = '0';

	SudokuSolver() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final char[][] board = { { '5', '3', '.', '.', '7', '.', '.', '.', '.' },
				{ '6', '.', '.', '1', '9', '5', '.', '.', '.' }, { '.', '9', '8', '.', '.', '.', '.', '6', '.' },
				{ '8', '.', '.', '.', '6', '.', '.', '.', '3' }, { '4', '.', '.', '8', '.', '3', '.', '.', '1' },
				{ '7', '.', '.', '.', '2', '.', '.', '.', '6' }, { '.', '6', '.', '.', '.', '.', '2', '8', '.' },
				{ '.', '.', '.', '4', '1', '9', '.', '.', '5' }, { '.', '.', '.', '.', '8', '.', '.', '7', '9' } };
		solveSudoku(board);
		final char[][] expected = { { '5', '3', '4', '6', '7', '8', '9', '1', '2' },
				{ '6', '7', '2', '1', '9', '5', '3', '4', '8' }, { '1', '9', '8', '3', '4', '2', '5', '6', '7' },
				{ '8', '5', '9', '7', '6', '1', '4', '2', '3' }, { '4', '2', '6', '8', '5', '3', '7', '9', '1' },
				{ '7', '1', '3', '9', '2', '4', '8', '5', '6' }, { '9', '6', '1', '5', '3', '7', '2', '8', '4' },
				{ '2', '8', '7', '4', '1', '9', '6', '3', '5' }, { '3', '4', '5', '2', '8', '6', '1', '7', '9' } };
		assert Arrays.deepEquals(board, expected);
	}

	static void solveSudoku(char[][] board) {
		final int m = board.length;
		final int n = board[0].length;
		final boolean[][] rows = new boolean[m][n + 1];
		final boolean[][] cols = new boolean[m][n + 1];
		final boolean[][] box = new boolean[m + 1][n + 1];
		for (int i = 0; i < m; i++) {
			for (int j = 0; j < n; j++) {
				if (board[i][j] != '.') {
					final int value = Integer.valueOf(board[i][j] + "");
					rows[i][value] = true;
					cols[j][value] = true;
					box[findBox(i, j)][value] = true;
				}
			}
		}
		backtrack(board, 0, rows, cols, box);
	}

	private static boolean backtrack(char[][] board, int cellNumber, boolean[][] rows, boolean[][] cols,
			boolean[][] box) {
		// answer found.
		if (cellNumber == 81)
			return true;

		// decode the cell number and find the coordinate point associated with it.
		final int i = cellNumber / 9;
		final int j = cellNumber - i * 9;
		if (board[i][j] == '.') {
			for (int v = 1; v < 10; v++) {
				if (!rows[i][v] && !cols[j][v] && !box[findBox(i, j)][v]) {
					// choose.
					board[i][j] = (char) (ZERO + v);
					rows[i][v] = true;
					cols[j][v] = true;
					box[findBox(i, j)][v] = true;
					// explore.
					if (backtrack(board, cellNumber + 1, rows, cols, box))
						return true;
					// unchoose.
					rows[i][v] = false;
					cols[j][v] = false;
					box[findBox(i, j)][v] = false;
					board[i][j] = '.';
				}
			}
		} else
			// explore. We have no choices here. These are just static values.
			return backtrack(board, cellNumber + 1, rows, cols, box);

		return false;
	}

	private static int findBox(int r, int c) {
		final int rowOffset = (r / 3);
		final int colOffset = (c / 3) + 1;
		return rowOffset * 3 + colOffset;
	}
}