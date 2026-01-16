package GeekBrians.Slava_5655380.Homework;

import static GeekBrians.Slava_5655380.Util.*;

import java.util.Random;
import java.util.Scanner;

public class Lesson_3 {
    public static void main(String[] args) {
        tasksFromFirstToFourth();
        System.out.println("-------------------------");
        taskFive();
    }

    public static void tasksFromFirstToFourth() {
        GomokuGame game = new GomokuGame(5, 4);
        game.play();
        System.out.println("Игра закончена.");
    }

    // ЗАДАНИЕ 5. *По желанию! Написать метод, который принимает на вход 2 целых числа, например, x и y.
    // Внутри метода создаётся целочисленный двумерный массив со сторонами х и у.
    // Потом этот массив заполняется последовательно инкрементируемыми числами по спирали (или змейкой).
    // То есть, в позиции [0,0] будет 1, в [0,1] 2 и так далее.
    // Потом по достижении правой границы направление заполнения меняется на вертикальное и так далее.
    // Так, что последний заполненный элемент будет посередине массива.
    public static void taskFive() {
        System.out.println("Введите значения X и Y для задания 5");
        Scanner scanner = new Scanner(System.in);
        final int X = scanner.nextInt(),
                Y = scanner.nextInt();
        int[][] matrix = new int[Y][X];
        for (int i = 1, bound = 0, column = 0, row = 0; i <= X * Y; i++) {
            matrix[row][column] = i;
            if (column < X - bound - 1 && row == bound)
                column++;
            else if (column == X - bound - 1 && row < Y - bound - 1)
                row++;
            else if (row == Y - bound - 1 && column > bound)
                column--;
            else if (column == bound && row > bound + 1)
                row--;
            else {
                bound++;
                column++;
            }
        }
        printArr(matrix);
    }
}

class GomokuGame {
    private final int SIZE;
    private char[][] map;
    private static final char DOT_EMPTY = '•';
    private static final char DOT_X = 'X';
    private static final char DOT_O = 'O';
    private final int DOTS_TO_WIN;
    private Scanner sc = new Scanner(System.in);
    private Random rand = new Random();
    private Vector2i lastTurn = new Vector2i();
    private int freeCellsCount;

    private void initMap() {
        map = new char[SIZE][SIZE];
        for (int i = 0; i < SIZE; i++) {
            for (int j = 0; j < SIZE; j++) {
                map[i][j] = DOT_EMPTY;
            }
        }
    }

    private void makeTurn(char symb, int column, int row) {
        map[row][column] = symb;
        freeCellsCount--;
        lastTurn.x = column;
        lastTurn.y = row;
    }

    // ЗАДАНИЕ 4. *** Доработать искусственный интеллект, чтобы он мог блокировать ходы игрока, и пытаться выиграть сам.
    private void aiTurn() {
        // Пробует заблокировать выигрышный ход игрока
        for (int i = 0; i < SIZE; i++)
            for (int j = 0; j < SIZE; j++) {
                if (map[i][j] == DOT_EMPTY) {
                    map[i][j] = (isUserFirst) ? DOT_X : DOT_O;
                    if (checkWin((isUserFirst) ? DOT_X : DOT_O, j, i)) {
                        System.out.println("Программа сделала ход в точку " + (j + 1) + " " + (i + 1));
                        makeTurn((isUserFirst) ? DOT_O : DOT_X, j, i);
                        return;
                    }
                    map[i][j] = DOT_EMPTY;
                }
            }
        // Иначе пробует найти выигрышный ход
        for (int i = 0; i < SIZE; i++)
            for (int j = 0; j < SIZE; j++) {
                if (map[i][j] == DOT_EMPTY) {
                    map[i][j] = (isUserFirst) ? DOT_O : DOT_X;
                    if (checkWin((isUserFirst) ? DOT_O : DOT_X, j, i)) {
                        System.out.println("Программа сделала ход в точку " + (j + 1) + " " + (i + 1));
                        makeTurn((isUserFirst) ? DOT_O : DOT_X, j, i);
                        return;
                    }
                    map[i][j] = DOT_EMPTY;
                }
            }
        // Иначе делает случайный ход
        int column, row;
        do {
            column = rand.nextInt(SIZE);
            row = rand.nextInt(SIZE);
        } while (!isCellValid(column, row));
        System.out.println("Программа сделала ход в точку " + (column + 1) + " " + (row + 1));
        makeTurn((isUserFirst) ? DOT_O : DOT_X, column, row);
    }

    private void humanTurn() {
        int column, row;
        do {
            System.out.println("Введите координаты в формате X Y");
            column = sc.nextInt() - 1;
            row = sc.nextInt() - 1;
        } while (!isCellValid(column, row));
        makeTurn((isUserFirst) ? DOT_X : DOT_O, column, row);
    }

    private boolean isCellValid(int x, int y) {
        if (x < 0 || x >= SIZE || y < 0 || y >= SIZE) return false;
        if (map[y][x] == DOT_EMPTY) return true;
        return false;
    }

    // ЗАДАНИЕ 2. Переделать проверку победы, чтобы она не была реализована просто набором условий.
    // ЗАДАНИЕ 3. Попробовать переписать логику проверки победы, чтобы она работала для поля 5х5 и количества фишек 4 в линию.
    private boolean checkWin(char symb) {
        return checkWin(symb, lastTurn.x, lastTurn.y);
    }

    private boolean checkWin(char symb, int x, int y) {
        // Возращается true если в последнем ходе образовывается горизонтальная линия состоящая из символов symb
        int lineLength = 0;
        // Обход вправо по линии
        for (int column = x; column < SIZE; column++, lineLength++) {
            if (map[y][column] != symb)
                break;
            if (lineLength == DOTS_TO_WIN)
                return true;
        }
        // Обход влево по линии
        for (int column = x; column >= 0; column--, lineLength++) {
            if (map[y][column] != symb)
                break;
            if (lineLength == DOTS_TO_WIN)
                return true;
        }
        // Возращается true если в последнем ходе образовывается вертикальная линия состоящая из символов symb
        lineLength = 0;
        // Обход вниз по линии
        for (int row = y; row < SIZE; row++, lineLength++) {
            if (map[row][x] != symb)
                break;
            if (lineLength == DOTS_TO_WIN)
                return true;
        }
        // Обход вверх по линии
        for (int row = y; row >= 0; row--, lineLength++) {
            if (map[row][x] != symb)
                break;
            if (lineLength == DOTS_TO_WIN)
                return true;
        }
        // Возращается true если в последнем ходе образовывается диоганальная линия похожая на \ и состоящая из символов symb
        lineLength = 0;
        // Обход вниз по линии
        for (int column = x, row = y; column < SIZE && row < SIZE; column++, row++, lineLength++) {
            if (map[row][column] != symb)
                break;
            if (lineLength == DOTS_TO_WIN)
                return true;
        }
        // Обходв вверх по линии
        for (int column = x, row = y; column >= 0 && row >= 0; column--, row--, lineLength++) {
            if (map[row][column] != symb)
                break;
            if (lineLength == DOTS_TO_WIN)
                return true;
        }
        // Возращается true если в последнем ходе образовывается диоганальная линия похожая на / и состоящая из символов symb
        lineLength = 0;
        // Обход вниз по линии
        for (int column = x, row = y; column >= 0 && row < SIZE; column--, row++, lineLength++) {
            if (map[row][column] != symb)
                break;
            if (lineLength == DOTS_TO_WIN)
                return true;
        }
        // Обходв вверх по линии
        for (int column = x, row = y; column < SIZE && row >= 0; column++, row--, lineLength++) {
            if (map[row][column] != symb)
                break;
            if (lineLength == DOTS_TO_WIN)
                return true;
        }

        return false;
    }

    private boolean isGameOver() {
        if (checkWin((isUserFirst) ? DOT_X : DOT_O)) {
            System.out.println("Победил человек");
            return true;
        }
        if (checkWin((isUserFirst) ? DOT_O : DOT_X)) {
            System.out.println("Победила программа");
            return true;
        }
        if (freeCellsCount == 0) {
            System.out.println("Ничья");
            return true;
        }
        return false;
    }

    public boolean isUserFirst = true;

    GomokuGame() {
        this(3, 3);
    }

    GomokuGame(int size, int dotsToWin) {
        SIZE = size;
        DOTS_TO_WIN = dotsToWin;
        initMap();
        freeCellsCount = SIZE * SIZE;
    }

    public boolean play() {
        if (isUserFirst)
            printArr(map);
        while (true) {
            if (isUserFirst) humanTurn();
            else aiTurn();
            printArr(map);
            if (isGameOver())
                return true;

            if (isUserFirst) aiTurn();
            else humanTurn();
            printArr(map);
            if (isGameOver())
                return false;
        }
    }
}