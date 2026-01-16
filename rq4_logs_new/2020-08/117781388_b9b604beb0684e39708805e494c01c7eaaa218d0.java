package Codility;

import java.util.LinkedList;
import java.util.Queue;

public class StrokesToPaint {
    public static void main(String[] args) {
        char[][] grid1 = {{'a', 'a', 'b', 'b', 'a'},
                {'a', 'a', 'b', 'b', 'a'},
                {'a', 'a', 'a', 'c', 'b'}};

        char[][] grid2 = {{'a', 'a', 'b', 'b', 'a'},
                {'a', 'b', 'a', 'b', 'a'},
                {'a', 'a', 'a', 'c', 'a'}};

        char[][] grid3 = {{'a', 'a', 'b', 'b', 'a'},
                {'a', 'a', 'b', 'b', 'a'},
                {'a', 'a', 'a', 'c', 'b'}};

        char[][] grid4 = {{'a', 'a', 'b', 'b', 'a'},
                {'a', 'b', 'a', 'b', 'a'},
                {'a', 'a', 'a', 'c', 'a'}};

        System.out.println(getStokesToPaintBFS(grid1));
        System.out.println(getStokesToPaintBFS(grid2));
        System.out.println(getStokesToPaintDFS(grid3));
        System.out.println(getStokesToPaintDFS(grid4));
    }

    static int[][] dirs = {{0, 1}, {0, -1}, {1, 0}, {-1, 0}};

    private static int getStokesToPaintDFS(char[][] grid) {
        int res = 0;
        for (int i = 0; i < grid.length; i++) {
            for (int j = 0; j < grid[0].length; j++) {
                if (grid[i][j] != '0') {
                    dfs(grid, i, j, grid[i][j]);
                    res++;
                }
            }
        }
        return res;
    }

    private static void dfs(char[][] grid, int i, int j, char c) {
        if (i < 0 || i >= grid.length || j < 0 || j >= grid[0].length || grid[i][j] == '0' || grid[i][j] != c)
            return;
        grid[i][j] = '0';
        for (int[] d : dirs) {
            int ni = i + d[0];
            int nj = j + d[1];
            dfs(grid, ni, nj, c);
        }
    }

    private static int getStokesToPaintBFS(char[][] grid) {
        int res = 0;
        for (int i = 0; i < grid.length; i++) {
            for (int j = 0; j < grid[0].length; j++) {
                if (grid[i][j] != '0') {
                    bfs(grid, i, j, grid[i][j]);
                    res++;
                }
            }
        }
        return res;
    }

    private static void bfs(char[][] grid, int i, int j, char c) {
        Queue<int[]> q = new LinkedList<>();
        q.offer(new int[]{i, j});
        while (!q.isEmpty()) {
            int[] cur = q.poll();
            grid[cur[0]][cur[1]] = '0';
            for (int[] d : dirs) {
                int ni = cur[0] + d[0];
                int nj = cur[1] + d[1];
                if (ni < 0 || ni >= grid.length || nj < 0 || nj >= grid[0].length || grid[ni][nj] == '0' || grid[ni][nj] != c)
                    continue;
                q.offer(new int[]{ni, nj});
            }
        }
    }

}