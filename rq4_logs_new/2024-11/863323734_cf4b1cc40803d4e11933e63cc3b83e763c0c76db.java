class Solution {
    public int minimumObstacles(int[][] grid) {
        int m = grid.length, n = grid[0].length;
        Deque<int[]> deque = new ArrayDeque<>();
        deque.offer(new int[] {0, 0, 0}); // {row, col, obstaclesRemoved}
        boolean[][] visited = new boolean[m][n];
        int[] dirs = {-1, 0, 1, 0, -1}; // to explore up, down, left, right

        while (!deque.isEmpty()) {
            int[] cur = deque.poll();
            int row = cur[0], col = cur[1], obstacles = cur[2];

            if (row == m - 1 && col == n - 1) {
                return obstacles; // Reached bottom-right corner
            }
            if (visited[row][col]) continue;
            visited[row][col] = true;

            for (int d = 0; d < 4; d++) {
                int newRow = row + dirs[d];
                int newCol = col + dirs[d + 1];
                if (newRow >= 0 && newRow < m && newCol >= 0 && newCol < n && !visited[newRow][newCol]) {
                    if (grid[newRow][newCol] == 0) {
                        deque.offerFirst(new int[] {newRow, newCol, obstacles});
                    } else {
                        deque.offerLast(new int[] {newRow, newCol, obstacles + 1});
                    }
                }
            }
        }
        return -1; // If no path exists
    }
}