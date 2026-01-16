package com.github.lucaseo90.medium;

public class No0079 {
    private final int[] dx = {-1, 1, 0, 0};
    private final int[] dy = {0, 0, -1, 1};

    public boolean exist(char[][] board, String word) {
        int m = board.length;
        int n = board[0].length;

        for (int i = 0; i < m; i++) {
            for (int j = 0; j < n; j++) {
                if (word.charAt(0) == board[i][j]) {
                    if (word.length() == 1) {
                        return true;
                    }
                    boolean[][] visited = new boolean[m][];
                    for (int k = 0; k < m; k++) {
                        visited[k] = new boolean[n];
                    }
                    visited[i][j] = true;
                    if (searchWord(board, m, n, word, visited, i, j, 0)) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    private boolean searchWord(char[][] board, int m, int n, String word, boolean[][] visited, int x, int y, int depth) {
        for (int k = 0; k < dx.length; k++) {
            int nextX = x + dx[k];
            int nextY = y + dy[k];
            if (nextX >= 0 && nextY >= 0 && nextX < m && nextY < n) {
                if (depth + 1 < word.length() && board[nextX][nextY] == word.charAt(depth + 1) && !visited[nextX][nextY]) {
                    visited[nextX][nextY] = true;
                    if (depth + 1 == word.length() - 1) {
                        return true;
                    }
                    boolean isExist = searchWord(board, m, n, word, visited, nextX, nextY, depth + 1);
                    if (isExist) {
                        return true;
                    } else {
                        visited[nextX][nextY] = false;
                    }
                }
            }
        }

        return false;
    }
}