package com.coding.challenge.bfs;

import java.util.ArrayDeque;
import java.util.Queue;

class ShortestPathInBinaryMatrix {
	private static final int[][] MOVES = { { -1, 0 }, { -1, 1 }, { 0, 1 }, { 1, 1 }, { 1, 0 }, { 1, -1 }, { 0, -1 },
			{ -1, -1 } };

	ShortestPathInBinaryMatrix() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final int[][] grid1 = { { 0, 1 }, { 1, 0 } };
		assert shortestPathBinaryMatrix(grid1) == 2;

		final int[][] grid2 = { { 0, 0, 0 }, { 1, 1, 0 }, { 1, 1, 0 } };
		assert shortestPathBinaryMatrix(grid2) == 4;

		final int[][] grid3 = { { 1, 0, 0 }, { 1, 1, 0 }, { 1, 1, 0 } };
		assert shortestPathBinaryMatrix(grid3) == -1;

		final int[][] grid4 = { { 0, 1, 1, 0, 0, 0 }, { 0, 1, 0, 1, 1, 0 }, { 0, 1, 1, 0, 1, 0 }, { 0, 0, 0, 1, 1, 0 },
				{ 1, 1, 1, 1, 1, 0 }, { 1, 1, 1, 1, 1, 0 } };
		assert shortestPathBinaryMatrix(grid4) == 14;

		final int[][] grid5 = { { 0, 0, 0 }, { 1, 1, 0 }, { 1, 1, 1 } };
		assert shortestPathBinaryMatrix(grid5) == -1;

		final int[][] grid6 = { { 0, 1, 1, 0, 1 }, { 0, 1, 0, 1, 0 }, { 0, 1, 0, 1, 0 }, { 1, 0, 1, 1, 0 },
				{ 1, 1, 1, 1, 0 } };
		assert shortestPathBinaryMatrix(grid6) == 11;
	}

	static int shortestPathBinaryMatrix(int[][] grid) {
		final int n = grid.length;
		final boolean[][] d = new boolean[n][n];
		final Queue<Block> q = new ArrayDeque<>();
		if (grid[0][0] == 0) {
			d[0][0] = true;
			q.offer(new Block(0, 0, 0));
		}
		while (!q.isEmpty()) {
			final Block blk = q.poll();
			if (blk.r == n - 1 && blk.c == n - 1)
				return blk.dist + 1;
			for (int[] m : MOVES) {
				final int r = blk.r + m[0];
				final int c = blk.c + m[1];
				if (r >= 0 && r < n && c >= 0 && c < n && grid[r][c] == 0 && !d[r][c]) {
					d[r][c] = true;
					q.offer(new Block(r, c, blk.dist + 1));
				}
			}
		}
		return -1;
	}

	static class Block {
		final int r;
		final int c;
		final int dist;

		Block(int r, int c, int dist) {
			this.r = r;
			this.c = c;
			this.dist = dist;
		}
	}
}