package graphProblem;

import java.util.List;

public class WordSearchHard {

    public static int numberOfApperance(char[][] board, List<String> words) {
        int counter = 0;
        for (String word : words) {
            counter += numberOfAppearances(board, word);
        }


        return counter;
    }

    private static int numberOfAppearances(char[][] board, String word) {
        int counter = 0;

        for (int i = 0; i < board.length; i++) {
            for (int j = 0; j < board[0].length; j++) {
                if (board[i][j] == word.charAt(0)) {
                    counter += explore(board, new boolean[board.length][board[0].length], i, j, true, 0, word);
                }


            }
        }


        return counter;

    }

    private static int explore(char[][] board, boolean[][] visited, int row, int column, boolean isInitial, int index, String word) {
        int count = 0;
        boolean isRowBound = row >= 0 && row < board.length;
        boolean isColumnBound = column >= 0 && column < board[0].length;

        if (!isRowBound || !isColumnBound || word.charAt(index) != board[row][column] || isInitial && visited[row][column])
            return 0;
        if (index == word.length() - 1) return 1;
        if (isInitial) {
            visited[row][column] = true;
            count += explore(board, visited, row + 1, column, false, index + 1, word);
            count += explore(board, visited, row, column + 1, false, index + 1, word);
        } else {

            count += explore(board, visited, row + 1, column, false, index + 1, word);
            count += explore(board, visited, row - 1, column, false, index + 1, word);
            count += explore(board, visited, row, column + 1, false, index + 1, word);

            count += explore(board, visited, row, column - 1, false, index + 1, word);

        }

        return count;

    }

    public static void main(String[] args) {
        char[][] board = {  {'o', 'c', 'a', 't'},
                            {'a', 'a', 'a', 't'},
                            {'o', 't', 'a', 'c'},
                            {'b', 'b', 't', 'a'}};

        System.out.println(numberOfApperance(board, List.of("cat")));
    }
}