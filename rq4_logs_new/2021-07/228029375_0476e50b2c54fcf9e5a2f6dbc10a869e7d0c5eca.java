package leetcode;

import java.util.stream.IntStream;

/**
 * This Game class simulates the Conway's game of life problem using a simple M*M char matrix
 */
public class Game {
    int length;
    int breadth;
    static char[][] initialGrid;

    public static void main(String[] args) {

        // The length, breadth and no of iterations can be changed from here
        Game gameOfLife = new Game(5, 5);
        gameOfLife.initializeMatrixForOscillator(5, 5);

        // Below line can be uncommented to play the game with random input
        //gameOfLife.initializeMatrixForRandomInput(10,20);

        gameOfLife.playGame(initialGrid, 10);
    }

    /**
     * Constructor
     *
     * @param length
     * @param breadth
     */
    public Game(int length, int breadth) {
        this.length = length;
        this.breadth = breadth;
    }

    /**
     * @param initialGrid
     * @param iterations
     */
    void playGame(char[][] initialGrid, int iterations) {
        displayCurrentGrid(initialGrid);
        while (iterations > 0) {
            initialGrid = nextGameState(initialGrid, length, breadth);
            displayCurrentGrid(initialGrid);
            iterations--;
        }
        System.out.println("Game Over");
    }


    /**
     * Changes the state of the current grid based on the individual elements' neighbours
     * @param grid
     * @param length
     * @param breadth
     * @return
     */
    char[][] nextGameState(char[][] grid, int length, int breadth) {

        char[][] newGrid = new char[length][breadth];
        for (int row = 0; row < length; row++)
            for (int column = 0; column < breadth; column++)
                newGrid[row][column] = changeStateBasedOnLiveNeighbourCount(grid, row, column);
        return newGrid;
    }

    /**
     * @param grid
     * @param length
     * @param breadth
     * @return
     */
    char changeStateBasedOnLiveNeighbourCount(char[][] grid, int length, int breadth) {
        int count = getLiveNeighboursCount(grid, length, breadth);
        if (grid[length][breadth] == '*') {
            if (count < 2 || count > 3)
                return '-';
            return '*';
        } else {
            if (count == 3)
                return '*';
            return '-';
        }
    }

    /**
     * @param grid
     * @param length
     * @param breadth
     * @return
     */
    int getLiveNeighboursCount(char[][] grid, int length, int breadth) {
        int count = 0;
        for (int x = length - 1; x <= length + 1; x++)
            for (int y = breadth - 1; y <= breadth + 1; y++) {
                if (x == length && y == breadth)
                    continue;
                count += getValue(grid, x, y);
            }
        return count;
    }

    /**
     * @param grid
     * @param row
     * @param column
     * @return
     */
    int getValue(char[][] grid, int row, int column) {
        if (row < 0 || column < 0 || row == length || column == breadth)
            return 0;
        if (grid[row][column] == '*')
            return 1;
        return 0;
    }

    /**
     * @param length
     * @param breadth
     */
    private void initializeMatrixForRandomInput(int length, int breadth) {
        initialGrid = new char[length][breadth];

        for (int i = 1; i < length; i++) {
            int randomRow = (int) (Math.random() * i);
            for (int j = 1; j < breadth; j++) {
                int randomColumn = (int) (Math.random() * j);
                initialGrid[randomRow][randomColumn] = '*';
            }


        }
        for (int row = 0; row < length; row++) {
            for (int column = 0; column < breadth; column++) {
                if (initialGrid[row][column] != '*')
                    initialGrid[row][column] = '-';
            }
        }
    }

    /**
     * @param length
     * @param breadth
     */
    private void initializeMatrixForOscillator(int length, int breadth) {
        initialGrid = new char[length][breadth];
        //initializing all the elements except the middle portion of the grid to '-'
        IntStream.range(0, length).forEach(i -> {
            IntStream.range(0, breadth).filter(j -> (i != 1 || i != 2 || i != 3) || j != 2).forEach(j -> {
                initialGrid[j][i] = '-';
            });

        });
        initialGrid[1][2] = '*';
        initialGrid[2][2] = '*';
        initialGrid[3][2] = '*';

    }

    /**
     * Displays all the elements in the grid
     *
     * @param grid
     */
    private void displayCurrentGrid(char[][] grid) {
        for (int row = 0; row < grid.length; row++) {
            for (int column = 0; column < grid[row].length; column++) {
                System.out.print(grid[row][column]);
            }
            System.out.println();
        }
        System.out.println();
    }
}