package com.github.lucaseo90.algo.days21.day6;

import java.util.LinkedList;
import java.util.Queue;

public class NumberOfIslands {
    final int[] dx = {-1, 1, 0, 0};
    final int[] dy = {0, 0, -1, 1};

    public int numIslands(char[][] grid) {
        int m = grid.length;
        int n = grid[0].length;

        int numIslands = 0;
        boolean[][] visited = new boolean[m][];
        for (int i = 0; i < m; i++) {
            visited[i] = new boolean[n];
        }

        for (int i = 0; i < m; i++) {
            for (int j = 0; j < n; j++) {
                if (!visited[i][j] && grid[i][j] == '1') {
                    numIslands += updateIslandSize(grid, visited, m, n, i, j);
                }
            }
        }

        return numIslands;
    }

    private int updateIslandSize(char[][] grid, boolean[][] visited, int m, int n, int startX, int startY) {
        Queue<Point> queue = new LinkedList<>();
        queue.add(new Point(startX, startY));
        visited[startX][startY] = true;

        while (!queue.isEmpty()) {
            Point p = queue.poll();
            int x = p.x;
            int y = p.y;

            for (int i = 0; i < dx.length; i++) {
                int nextX = x + dx[i];
                int nextY = y + dy[i];

                if (nextX >= 0 && nextY >= 0 && nextX < m && nextY < n) {
                    if (!visited[nextX][nextY] && grid[nextX][nextY] == '1') {
                        visited[nextX][nextY] = true;
                        queue.add(new Point(nextX, nextY));
                    }
                }

            }
        }
        return 1;
    }

    private class Point {
        int x;
        int y;

        public Point(int x, int y) {
            this.x = x;
            this.y = y;
        }
    }
}