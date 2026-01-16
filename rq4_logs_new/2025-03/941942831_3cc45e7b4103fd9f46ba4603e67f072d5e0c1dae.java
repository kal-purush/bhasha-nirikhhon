package Lecture3;

public class KnightsTour {
    private static final int[] rowMoves = {2, 1, -1, -2, -2, -1, 1, 2};
    private static final int[] colMoves = {1, 2, 2, 1, -1, -2, -2, -1};
    private static final int N = 8; // Change N for a different board size

    public static void main(String[] args) {
        int[][] board = new int[N][N];

        // Initialize the board with -1 (unvisited)
        for (int i = 0; i < N; i++) {
            for (int j = 0; j < N; j++) {
                board[i][j] = -1;
            }
        }

        board[0][0] = 0; // Start at the first cell
        if (solveKnightTour(board, 0, 0, 1)) {
            printBoard(board);
        } else {
            System.out.println("No solution exists.");
        }
    }

    private static boolean solveKnightTour(int[][] board, int row, int col, int moveNum) {
        if (moveNum == N * N) return true; // All cells visited

        for (int i = 0; i < 8; i++) {
            int nextRow = row + rowMoves[i];
            int nextCol = col + colMoves[i];

            if (isValidMove(board, nextRow, nextCol)) {
                board[nextRow][nextCol] = moveNum; // Make the move
                if (solveKnightTour(board, nextRow, nextCol, moveNum + 1)) return true;
                board[nextRow][nextCol] = -1; // Backtrack
            }
        }
        return false;
    }

    private static boolean isValidMove(int[][] board, int row, int col) {
        return row >= 0 && col >= 0 && row < N && col < N && board[row][col] == -1;
    }

    private static void printBoard(int[][] board) {
        for (int[] row : board) {
            for (int cell : row) {
                System.out.printf("%2d ", cell);
            }
            System.out.println();
        }
    }
}
