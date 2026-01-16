package ch1;

import util.Question;

public class BlankMatrix extends Question {

    public BlankMatrix() { super("1.7", "Given an MxN matrix, set row and col to 0 if an element in that row or col is 0."); }

    int[][] a = {{1,0,3,4}, {5,6,7,8}, {9,10,11,12}, {13,14,15,16}, {17, 18, 19, 0}};

    int[][] b = {{1,2,0, 10}, {4,5,6, 11}, {0,8,9, 12}};

    int[][] c ={{0,2}, {3, 4}, {5, 6}};

    int[][] d = {{1,2,3,4, 0}, {0,7,8, 9, 10}, {11,12, 13,14,15 }, {16, 17, 18, 19, 20}, {21, 22, 23, 24, 0}};

    @Override
    public void run() {
        hello();

        System.out.println("Initial:");
        printInt2D(c);
        blankMatrix(c);
        System.out.println("Blanked:");
        printInt2D(c);

        System.out.println("Initial:");
        printInt2D(b);
        blankMatrix(b);
        System.out.println("Blanked:");
        printInt2D(b);

        System.out.println("Initial:");
        printInt2D(a);
        System.out.println("Blanked:");
        blankMatrix(a);
        printInt2D(a);

        System.out.println("Initial:");
        printInt2D(d);
        System.out.println("Blanked:");
        blankMatrix(d);
        printInt2D(d);

        bye();
    }

    public void blankMatrix(int[][] in) {
        if (in.length == 0) {
            return;
        }

        boolean[] rowMarker = new boolean[in.length];
        boolean[] colMarker = new boolean[in[0].length];

        for (int row = 0; row < in.length; row++) {
            for (int col = 0; col < in[row].length; col++) {
                if (in[row][col] == 0) {
                    rowMarker[row] = true;
                    colMarker[col] = true;
                }
            }
        }

        for (int row = 0; row < in.length; row++) {
            for (int col = 0; col < in[row].length; col++) {
                if (rowMarker[row] || colMarker[col]) {
                    in[row][col] = 0;
                }
            }
        }
    }
}