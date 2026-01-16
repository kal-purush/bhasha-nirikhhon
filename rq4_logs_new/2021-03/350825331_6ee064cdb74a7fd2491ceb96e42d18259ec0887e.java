package tictactoe;
import java.util.Arrays;
import java.util.Scanner;

public class Main {


    Scanner scanner = new Scanner(System.in);
    char[][] array = {{' ', ' ', ' '}, {' ', ' ', ' '}, {' ', ' ', ' '}};
    int[] coordinate = new int[2];
    char winner = 'N';
    boolean playerX = true;
    int counterX = 0;
    int counterO = 0;
    boolean run = true;


    public void game() {
        buildField();
        while (run) {
            readCoordinates();
            buildField();
            gameRules();
        }
    }


    public void buildField() {


        System.out.println("---------");

        for (int i = 0; i < 3; i++) {
            System.out.print("| ");
            for (int j = 0; j < 3; j++) {
                System.out.print(array[i][j] + " ");
            }
            System.out.println("|");
        }
        System.out.println("---------");
    }

    public void readCoordinates() {
        boolean inputCorrect = false;
        //int[] input = new int[2];

        do {
            System.out.print("Enter the coordinates: ");

            try {
                String str = scanner.nextLine();
                coordinate = Arrays.stream(str.split(" ")).mapToInt(Integer :: parseInt).toArray();;

                if (coordinate[0] >= 1 && coordinate[0] <= 3) {
                    if (coordinate[1] >= 1 && coordinate[1] <= 3) {
                        inputCorrect = true;
                    } else {
                        System.out.println("Coordinates should be from 1 to 3!");
                    }
                } else {
                    System.out.println("Coordinates should be from 1 to 3!");
                }

            } catch (NumberFormatException e) {
                //log.error(e);
                System.out.println("You should enter numbers!");
            } catch (IllegalArgumentException e) {
                //log.error(e)
                System.out.println("You should enter numbers!");
            }
        } while (!inputCorrect);
        setCoordinate();
    }


    public void setCoordinate() {
        if (array[coordinate[0] - 1][coordinate[1] - 1] == 'X') {
            System.out.println("This cell is occupied! Choose another one!");
            readCoordinates();
        } else if (array[coordinate[0] - 1][coordinate[1] - 1] == 'O') {
            System.out.println("This cell is occupied! Choose another one!");
            readCoordinates();
        } else {
            if (playerX) {
                array[coordinate[0] - 1][coordinate[1] - 1] = 'X';
                counterX++;
                playerX = false;
            } else {
                array[coordinate[0] - 1][coordinate[1] - 1] = 'O';
                counterO++;
                playerX = true;
            }
        }
    }


    public void gameRules() {

        boolean draw = false;
        char test = 'X';
        for (int j = 0; j < 2; j++) {
            for (int i = 0; i < array.length; i++) {
                if (array[i][0] == test && array[i][1] == test && array[i][2] == test) {
                    winner = test;
                }
            }
            for (int i = 0; i < array.length; i++) {
                if (array[0][i] == test && array[1][i] == test && array[2][i] == test) {
                    winner = test;
                }
            }
            if (array[0][0] == test && array[1][1] == test && array[2][2] == test) {
                winner = test;
            }
            if (array[0][2] == test && array[1][1] == test && array[2][0] == test) {
                winner = test;
            }
            test = 'O';
        }



        if ((winner == 'X' || winner == 'O')) {
            System.out.println(winner + " wins");
            if (counterX == counterO) {
                run = false;

            }
        }

        if (counterX == 5 && winner == 'N') {
            run = false;
            draw = true;
        }

        if (counterX == 5) {
            run = false;
        }

        if (draw) {
            System.out.println("Draw");
            run = false;
        }


    }

    public static void main(String[] args) {
        // write your code here

        Main myProgram = new Main();
        myProgram.game();
    }

}