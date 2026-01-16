package BreadthFirstSearch.TwoDArrays;

import java.util.LinkedList;
import java.util.Queue;

public class TheMaze {
    public boolean hasPath(int[][] maze, int[] start, int[] destination) {
        int row = maze.length;
        int col = maze[0].length;

        boolean[][] visited = new boolean[row][col];
        int[] dx = new int[]{0, -1, 0, 1};
        int[] dy = new int[]{1, 0, -1, 0};
        Queue<int[]> queue = new LinkedList<>();
        queue.offer(start);
        visited[start[0]][start[1]] = true;

        while (!queue.isEmpty()) {
            int[] position = queue.poll();
            if (position[0] == destination[0] && position[1] == destination[1]) {
                return true;
            }

            for (int direction = 0; direction < 4; direction++) {
                int nextX = position[0], nextY = position[1];
                while (inBound(maze, nextX, nextY) && maze[nextX][nextY] == 0) {
                    nextX += dx[direction];
                    nextY += dy[direction];
                }

                nextX -= dx[direction];
                nextY -= dy[direction];

                if (!visited[nextX][nextY]) {
                    queue.offer(new int[]{nextX, nextY});
                    visited[nextX][nextY] = true;
                }
            }
        }

        return false;
    }

    public boolean inBound(int[][] maze, int x, int y) {
        int row = maze.length;
        int col = maze[0].length;
        return x >= 0 && x < row && y >= 0 && y < col;
    }
}