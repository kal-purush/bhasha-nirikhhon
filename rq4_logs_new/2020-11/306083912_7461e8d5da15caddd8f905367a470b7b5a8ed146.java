import javax.swing.*;
import java.util.Random;
import java.util.Scanner;

public class Homework_3 {
    public static char[][] map;
    public static final int SIZE = 3;
    public static final int CELLS_TO_WIN = 3;

    public static final char EMPTY_CELL = '•';
    public static final char X_CELL = 'X';
    public static final char O_CELL = 'O';

    public static Scanner scan = new Scanner(System.in);
    public static Random rand = new Random();

    public static void main(String[] args) {
        initMap();
        printMap();

        while (true) {
            humanTurn();
            printMap();

            if (checkWin(X_CELL)) {
                System.out.println("Human wins");
                break;
            }

            if (isMapFull()) {
                System.out.println("It's a draw");
                break;
            }

            aiTurn();
            printMap();

            if (checkWin(O_CELL)) {
                System.out.println("Skynet wins");
                break;
            }

            if (isMapFull()) {
                System.out.println("It's a draw");
                break;
            }
        }

        System.out.println("Game Over");
    }

    public static void initMap() {
        map = new char[SIZE][SIZE];
        for (int i = 0; i < SIZE; i++) {
            for (int j = 0; j < SIZE; j++) {
                map[i][j] = EMPTY_CELL;
            }
        }
    }

    public static void printMap() {
        for (int i = 0; i < SIZE; i++) {
            for (int j = 0; j < SIZE; j++) {
                System.out.print(map[i][j] + " ");
            }
            System.out.println();
        }
    }

    public static boolean isCellValid(int x, int y) {
        if (x < 0 || x >= SIZE || y < 0 || y >= SIZE) return false;
        return map[y][x] == EMPTY_CELL;
    }

    public static void humanTurn() {
        int x, y;
        do {
            System.out.println("Enter X and Y coordinates");
            x = scan.nextInt() - 1;
            y = scan.nextInt() - 1;
        } while (!isCellValid(x, y));
        map[y][x] = X_CELL;
    }

    public static void aiTurn() {
        int x, y;
        do {
            x = rand.nextInt(SIZE);
            y = rand.nextInt(SIZE);
        } while (!isCellValid(x, y));
        map[y][x] = O_CELL;
    }

    public static boolean checkWin (char symbol) {
        int hWin = 0, vWin = 0, bDiagonalWin = 0, fDiagonalWin = 0;

        for (int i = 0; i < SIZE; i++){
            for (int j = 0; j < SIZE; j++){
                if (map[i][j] == symbol) hWin++; //horizontal
                if (map[j][i] == symbol) vWin++; //vertical
            }

            if (hWin == SIZE || vWin == SIZE) return true;
            hWin = 0; vWin = 0;

            if (map[i][i] == symbol) bDiagonalWin++; // \
            if (map[i][SIZE - 1 - i] == symbol) fDiagonalWin++; // /
        }

        return (bDiagonalWin == SIZE || fDiagonalWin == SIZE);

//        int i, j;
//        //rows
//        for (i = 0; i < SIZE; i++) {
//            for (j = 0; j < SIZE; j++) {
//                if (map[i][j] != symbol) break;
//            }
//
//            if (j == SIZE) return true;
//        }
//
//        //columns
//        for (i = 0; i < SIZE; i++) {
//            for (j = 0; j < SIZE; j++) {
//                if(map[j][i] != symbol) break;
//            }
//
//            if (j == SIZE) return true;
//        }
//
//        //backward diagonal
//        for (i = 0; i < SIZE; i++) {
//            if (map[i][i] != symbol) break;
//        }
//
//        if (i == SIZE) return true;
//
//        //forward diagonal
//        for (i = 0; i < SIZE; i++){
//            if(map[i][SIZE - 1 - i] != symbol) break;
//        }
//
//        return i == SIZE;
    }

    public static boolean isMapFull() {
        for (int i = 0; i < SIZE; i++) {
            for (int j = 0; j < SIZE; j++) {
                if (map[i][j] == EMPTY_CELL) return false;
            }
        }
        return true;
    }
}