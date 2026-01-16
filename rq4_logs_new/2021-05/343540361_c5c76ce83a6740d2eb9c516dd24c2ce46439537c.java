package problem52;

public final class NQueens2 {

    public int totalNQueens(final int n) {
        if (n == 1)
            return 1;

        if (n <= 3)
            return 0;

        final boolean[][] board = new boolean[n][n];
        return recHelper(board, 0, n);
    }

    private int recHelper(final boolean[][] board, final int lev, final int n) {
        int comb = 0;

        col: for (int c = 0; c < n; c++) {

            // check cols
            for (int r = 0; r < lev; r++)
                if (board[r][c])
                    continue col;

            // check left diag
            for (int rd = lev - 1, cd = c - 1; rd >= 0 && cd >= 0; rd--, cd--)
                if (board[rd][cd])
                    continue col;

            // check right diag
            for (int rd = lev - 1, cd = c + 1; rd >= 0 && cd < n; rd--, cd++)
                if (board[rd][cd])
                    continue col;

            if (lev == n - 1) {
                comb++;
            } else {
                board[lev][c] = true;
                comb += recHelper(board, lev + 1, n);
                board[lev][c] = false;
            }
        }

        return comb;
    }

}