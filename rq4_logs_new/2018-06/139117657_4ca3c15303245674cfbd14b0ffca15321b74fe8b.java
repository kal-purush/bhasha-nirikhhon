package com.gojek.GojekGol;

public class Cell {

    public  int calculateNeighbors(int[][] block, int rowIndex, int colIndex) {
        int sum = 0;

        if(rowIndex - 1 >= 0) {
            if(colIndex - 1 >= 0) {
                sum += block[rowIndex - 1][colIndex - 1];
            }

            sum += block[rowIndex - 1][colIndex];

            if(colIndex + 1 < block[0].length) {
                sum += block[rowIndex - 1][colIndex + 1];
            }
        }

        if(colIndex - 1 >= 0) {
            sum += block[rowIndex][colIndex - 1];
        }

        if(colIndex + 1 < block[0].length) {
            sum += block[rowIndex][colIndex + 1];
        }

        if(rowIndex + 1 < block.length) {
            if(colIndex - 1 >= 0) {
                sum += block[rowIndex + 1][colIndex - 1];
            }

            sum += block[rowIndex + 1][colIndex];

            if(colIndex + 1 < block[0].length) {
                sum += block[rowIndex + 1][colIndex + 1];
            }
        }

        return sum;
    }

    public int livesRules(int[][] life, int rowIndex, int colIndex) {
        int result = calculateNeighbors(life, rowIndex, colIndex);
        if(result > 3 || result < 2) {
            return 0;
        } else {
            return 1;
        }
    }

    public int reproduce(int[][] life, int rowIndex, int colIndex) {
        Cell cells = new Cell();
        int result = cells.calculateNeighbors(life, rowIndex, colIndex);

        if(result == 3) {
            return 1;
        } else {
            return 0;
        }
    }
    
}