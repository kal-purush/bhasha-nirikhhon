package com.coding.challenge.matrix;

class ValidSudoku {
	ValidSudoku() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final char[][] board1 = { { '5', '3', '.', '.', '7', '.', '.', '.', '.' },
				{ '6', '.', '.', '1', '9', '5', '.', '.', '.' }, { '.', '9', '8', '.', '.', '.', '.', '6', '.' },
				{ '8', '.', '.', '.', '6', '.', '.', '.', '3' }, { '4', '.', '.', '8', '.', '3', '.', '.', '1' },
				{ '7', '.', '.', '.', '2', '.', '.', '.', '6' }, { '.', '6', '.', '.', '.', '.', '2', '8', '.' },
				{ '.', '.', '.', '4', '1', '9', '.', '.', '5' }, { '.', '.', '.', '.', '8', '.', '.', '7', '9' } };
		assert isValidSudoku(board1);

		final char[][] board2 = { { '8', '3', '.', '.', '7', '.', '.', '.', '.' },
				{ '6', '.', '.', '1', '9', '5', '.', '.', '.' }, { '.', '9', '8', '.', '.', '.', '.', '6', '.' },
				{ '8', '.', '.', '.', '6', '.', '.', '.', '3' }, { '4', '.', '.', '8', '.', '3', '.', '.', '1' },
				{ '7', '.', '.', '.', '2', '.', '.', '.', '6' }, { '.', '6', '.', '.', '.', '.', '2', '8', '.' },
				{ '.', '.', '.', '4', '1', '9', '.', '.', '5' }, { '.', '.', '.', '.', '8', '.', '.', '7', '9' } };
		assert !isValidSudoku(board2);
	}

	static boolean isValidSudoku(char[][] board) {
		final int m = board.length;
		final int n = board[0].length;
		final boolean[][] rows = new boolean[m][n + 1];
		final boolean[][] cols = new boolean[m][n + 1];
		final boolean[][] box = new boolean[m + 1][n + 1];
		for (int i = 0; i < m; i++) {
			for (int j = 0; j < n; j++) {
				if (board[i][j] != '.') {
					final int value = Integer.valueOf(board[i][j] + "");
					if (rows[i][value] || cols[j][value] || box[findBox(i, j)][value])
						return false;
					rows[i][value] = true;
					cols[j][value] = true;
					box[findBox(i, j)][value] = true;
				}
			}
		}
		return true;
	}

	private static int findBox(int r, int c) {
		final int rowOffset = (r / 3);
		final int colOffset = (c / 3) + 1;
		return rowOffset * 3 + colOffset;
	}
}