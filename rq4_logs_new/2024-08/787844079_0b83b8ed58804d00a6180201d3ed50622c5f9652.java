//https://leetcode.com/problems/valid-sudoku/
package algorithms.medium;

import java.util.HashSet;

public class ValidSudoku {
    public boolean isValidSudoku(char[][] board) {
        HashSet<String> set = new HashSet<>();
        for (int row = 0; row < 9; row++) {
            for (int col = 0; col < 9; col++) {
                char ch = board[row][col];
                if (ch != '.') {
                    if (!set.add(ch + "found in row " + row)
                            || !set.add(ch + "found in col " + col)
                            || !set.add(ch + "found in box " + row / 3 + "-" + col / 3))
                        return false;
                }
            }
        }
        return true;
    }
}