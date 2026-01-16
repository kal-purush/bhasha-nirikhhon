class Solution {
    public int minimumTime(int[][] grid) {
        if (grid[0][1] > 1 && grid[1][0] > 1) {
            return -1; // If starting movement is blocked
        }

        int m = grid.length, n = grid[0].length;
        int[][] directions = {{0, 1}, {1, 0}, {0, -1}, {-1, 0}};
        PriorityQueue<int[]> pq = new PriorityQueue<>((a, b) -> a[0] - b[0]);
        pq.offer(new int[]{0, 0, 0}); // {time, row, col}
        boolean[][] visited = new boolean[m][n];

        while (!pq.isEmpty()) {
            int[] current = pq.poll();
            int time = current[0], row = current[1], col = current[2];

            if (row == m - 1 && col == n - 1) {
                return time; // Reached bottom-right
            }

            if (visited[row][col]) continue;
            visited[row][col] = true;

            for (int[] dir : directions) {
                int newRow = row + dir[0], newCol = col + dir[1];
                if (newRow < 0 || newRow >= m || newCol < 0 || newCol >= n || visited[newRow][newCol]) {
                    continue;
                }
                int waitTime = (grid[newRow][newCol] - time) % 2 == 0 ? 1 : 0;
                int nextTime = Math.max(time + 1, grid[newRow][newCol] + waitTime);
                pq.offer(new int[]{nextTime, newRow, newCol});
            }
        }
        return -1; // If the destination is unreachable
    }
}