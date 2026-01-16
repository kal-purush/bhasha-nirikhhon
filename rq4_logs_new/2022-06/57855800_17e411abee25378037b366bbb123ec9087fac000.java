package com.github.lucaseo90.easy;

import java.util.LinkedList;
import java.util.Queue;

public class No0733 {
    private final int[] dx = {1, -1, 0, 0};
    private final int[] dy = {0, 0, 1, -1};

    public int[][] floodFill(int[][] image, int sr, int sc, int newColor) {
        int n = image.length;
        int m = image[0].length;

        boolean[][] visited = new boolean[n][];
        for (int i = 0; i < n; i++) {
            visited[i] = new boolean[m];
        }

        Queue<Point> queue = new LinkedList<>();
        queue.add(new Point(sr, sc));

        while (!queue.isEmpty()) {
            Point point = queue.poll();
            int x = point.x;
            int y = point.y;

            visited[x][y] = true;

            for (int i = 0; i < dx.length; i++) {
                int dx = x + this.dx[i];
                int dy = y + this.dy[i];

                if (dx >= 0 && dy >= 0 && dx < n && dy < m) {
                    if (!visited[dx][dy] && image[x][y] == image[dx][dy]) {
                        queue.add(new Point(dx, dy));
                    }
                }
            }
            image[x][y] = newColor;
        }

        return image;
    }

    private class Point {
        private int x;
        private int y;

        public Point(int x, int y) {
            this.x = x;
            this.y = y;
        }
    }
}