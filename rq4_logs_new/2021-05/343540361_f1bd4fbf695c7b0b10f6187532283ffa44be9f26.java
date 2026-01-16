package problem51;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public final class NQueens {

    public List<List<String>> solveNQueens(final int n) {
        final StringBuilder sb = new StringBuilder(n);
        final char[][] board = new char[n][n];

        for (int i = 0; i < n; i++)
            for (int j = 0; j < n; j++)
                board[i][j] = '.';

        if (n == 1) {
            board[0][0] = 'Q';
            return List.of(boardToList(board, sb, n));
        } else if (n <= 3) {
            return List.of();
        }

        final List<List<String>> result = new LinkedList<>();
        solve(board, result, n, 0, sb);
        return result;
    }

    private void solve(final char[][] board, final List<List<String>> result, final int n, final int level, final StringBuilder helperBuilder) {
        col: for (int c = 0; c < n; c++) {
            // first, check if there is anything in the same col
            for (int r = 0; r < level; r++)
                if (board[r][c] == 'Q')
                    continue col;

            // check diagonals
            for (int rd = level - 1, cd = c - 1; rd >= 0 && cd >= 0; rd--, cd--)
                if (board[rd][cd] == 'Q')
                    continue col;

            for (int rd = level - 1, cd = c + 1; rd >= 0 && cd < n; rd--, cd++)
                if (board[rd][cd] == 'Q')
                    continue col;

            // place the queen and go rec
            board[level][c] = 'Q';
            solve(board, result, n, level + 1, helperBuilder);

            if (level == n - 1)
                result.add(boardToList(board, helperBuilder, n));

            // remove the queen
            board[level][c] = '.';
        }
    }

    private static List<String> boardToList(final char[][] board, final StringBuilder sb, final int n) {
        final List<String> boardAsList = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            sb.setLength(0);
            for (int j = 0; j < n; j++)
                sb.append(board[i][j]);
            boardAsList.add(sb.toString());
        }
        return boardAsList;
    }

}