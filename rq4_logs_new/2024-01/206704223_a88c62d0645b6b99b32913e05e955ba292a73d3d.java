package com.coding.challenge.backtracking;

class WordSearch {
	private static int[][] MOVES = { { 0, 1 }, { 0, -1 }, { -1, 0 }, { 1, 0 } };

	WordSearch() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final char[][] board1 = { { 'A', 'B', 'C', 'E' }, { 'S', 'F', 'C', 'S' }, { 'A', 'D', 'E', 'E' } };
		assert exist(board1, "ABCCED");

		final char[][] board2 = { { 'A', 'B', 'C', 'E' }, { 'S', 'F', 'C', 'S' }, { 'A', 'D', 'E', 'E' } };
		assert exist(board2, "SEE");

		final char[][] board3 = { { 'A', 'B', 'C', 'E' }, { 'S', 'F', 'C', 'S' }, { 'A', 'D', 'E', 'E' } };
		assert !exist(board3, "ABCB");

		final char[][] board4 = { { 'a' } };
		assert exist(board4, "a");
	}

	static boolean exist(char[][] board, String word) {
		final int m = board.length;
		final int n = board[0].length;
		final boolean[][] d = new boolean[m][n];
		for (int i = 0; i < m; i++)
			for (int j = 0; j < n; j++)
				if (backtrack(board, word, i, j, 0, d))
					return true;
		return false;
	}

	private static boolean backtrack(char[][] board, String word, int r, int c, int k, boolean[][] d) {
		if (k == word.length() - 1)
			return board[r][c] == word.charAt(k);
		if (board[r][c] != word.charAt(k))
			return false;

		// choose.
		d[r][c] = true;
		final int m = board.length;
		final int n = board[0].length;
		// explore
		for (int[] move : MOVES) {
			final int i = r + move[0];
			final int j = c + move[1];
			if (i >= 0 && i < m && j >= 0 && j < n && !d[i][j])
				if (backtrack(board, word, i, j, k + 1, d))
					return true;
		}
		// unchoose.
		d[r][c] = false;
		return false;
	}
}