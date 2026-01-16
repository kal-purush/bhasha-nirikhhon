package POTD;

import java.util.LinkedList;
import java.util.Queue;

public class MinimumDaysToDisconnectIslands {

	static class Pair {
		int first;
		int second;

		Pair(int first, int second) {
			this.first = first;
			this.second = second;
		}
	}

	public int minDays(int[][] grid) {
		int n = grid.length;
		int m = grid[0].length;
		boolean[][] visited = new boolean[n][m];
		int count = 0;

		for(int i = 0; i < n; i++) {
			for(int j = 0; j < m; j++) {
				if(!visited[i][j] && grid[i][j] == 1) {
					bfs(i, j, grid, visited);
					count++;
				}
			}
		}

		if(count == 0 || count > 1) return 0;

		for(int i = 0; i < n; i++) {
			for(int j = 0; j < m; j++) {
				if(grid[i][j] == 1) {
					grid[i][j] = 0;
					int islands = checkIslands(grid);
					if(islands >= 2 || islands == 0) return 1;
					grid[i][j] = 1;
				}
			}
		}

		return 2;
	}

	public int checkIslands(int[][] grid) {
		int n = grid.length;
		int m = grid[0].length;
		boolean[][] visited = new boolean[n][m];
		int count = 0;

		for(int i = 0; i < n; i++) {
			for(int j = 0; j < m; j++) {
				if(!visited[i][j] && grid[i][j] == 1) {
					bfs(i, j, grid, visited);
					count++;
				}
			}
		}

		return count;
	}

	public void bfs(int r, int c, int[][] grid, boolean[][] visited) {
		Queue<Pair> q = new LinkedList<>();
		visited[r][c] = true;
		q.add(new Pair(r, c));

		int[] delRow = {-1, 0, 1, 0};
		int[] delCol = {0, 1, 0, -1};

		while(!q.isEmpty()) {
			int row = q.peek().first;
			int col = q.peek().second;
			q.poll();

			for(int i = 0; i < 4; i++) {
				int nrow = row + delRow[i];
				int ncol = col + delCol[i];

				if(nrow >= 0 && ncol >= 0 && nrow < grid.length && ncol < grid[0].length && !visited[nrow][ncol] && grid[nrow][ncol] == 1) {
					q.add(new Pair(nrow, ncol));
					visited[nrow][ncol] = true;
				}
			}
		}
	}

	public static void main(String[] args) {

	}
}