package com.github.lucaseo90.medium;

import java.util.LinkedList;
import java.util.Queue;
import java.util.Stack;

public class No0994 {
    private final int[] dx = {1, -1, 0, 0};
    private final int[] dy = {0, 0, 1, -1};

    public int orangesRotting(int[][] grid) {
        int m = grid.length;
        int n = grid[0].length;

        boolean[][] visited = new boolean[m][];
        int[][] rottedMap = new int[m][];
        Queue<Point> queue = new LinkedList<>();
        int day = 0;
        for (int i = 0; i < m; i++) {
            visited[i] = new boolean[n];
            rottedMap[i] = new int[n];
            for (int j = 0; j < n; j++) {
                if (grid[i][j] == 2) {
                    queue.add(new Point(i, j));
                    visited[i][j] = true;
                }
            }
        }

        while (!queue.isEmpty()) {
            Point point = queue.poll();
            int x = point.x;
            int y = point.y;

            for (int k = 0; k < dx.length; k++) {
                int dx = x + this.dx[k];
                int dy = y + this.dy[k];

                if(dx >= 0 && dy >= 0 && dx < m && dy < n) {
                    if (!visited[dx][dy] && grid[dx][dy] == 1) {
                        visited[dx][dy] = true;
                        rottedMap[dx][dy] = rottedMap[x][y] + 1;
                        day = Math.max(rottedMap[x][y], rottedMap[dx][dy]);
                        queue.add(new Point(dx, dy));
                    }
                }
            }
        }

        for (int i = 0; i < m; i++) {
            for (int j = 0; j < n; j++) {
                if(!visited[i][j] && grid[i][j] == 1) {
                    day = -1;
                    break;
                }
            }
        }

        return day;
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