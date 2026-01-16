package homeWork.fourthLesson;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;
/**
 * Important Point input need be 11 21 22 for example because x and y will be calculated:
 * for example:
 * 11 first '1' is for X and second '1' is for Y
 * 21 first '2' is for X and second '1' is for Y
 * 32 means x = 3 , y = 2 -> algorithm calculated alone from number 32 x and y.
 * Input must be concatenated in one integer.
 * */
public class Main {
    public static void main(String[] args) {
        startGame();
    }

    static void startGame() {
        /** Game cycle:*/
        System.out.println("Hi welcome to X-O game!");
        System.out.println("Choose matrix side:(3/5):");
        char[][] gamePlace = initGamePlace(getSize()); // Select game Size
        printMatrix(gamePlace); // print matrix
        int steps = gamePlace.length * gamePlace.length; // Calculate game steps

        for (int i = 0; i < steps / 2 + 1; i++) { // Steps iteration

            int[] data = humanStep(gamePlace); // human step
            printMatrix(gamePlace);

            if (controlGame(gamePlace) == 0) { // game control
                printMatrix(gamePlace);
                break;
            }
            pcSteps(gamePlace, data); // pc step
            printMatrix(gamePlace);
            if (controlGame(gamePlace) == 0) { // game control
                printMatrix(gamePlace);
                break;
            }
        }
    }

    private static void pcSteps(char[][] matrix, int[] data) {
        /**
         * Computer Step
         * */
        boolean step = false; // show if first if was not happened
        if (matrix.length == 3) { // are condition for 3x3 to take middle
            if (matrix[1][1] == '•') {
                matrix[1][1] = 'O';
                step = true;
            } else if (data[0] == 2 && data[1] == 2) { // condition if middle is taken to take corners
                checkCorners(matrix);
                step = true;
            }
        }
        if (step == false) {
            int[] coordinates = checkSteps(matrix); // coordinates has data which is used for algorithm logic
            if (coordinates[0] == -1 && coordinates[1] == -1) { // is not dangerous select random position!
                int x;
                int y;
                do {
                    x = new Random().nextInt(matrix.length);
                    y = new Random().nextInt(matrix.length);
                } while (matrix[x][y] != '•');
                matrix[x][y] = 'O';
            } else if (coordinates[0] == -1 && coordinates[1] == 0) { // shows position is on diagonal left-> right
                for (int i = 0; i < matrix.length; i++) {
                    if (matrix[i][i] == '•') {
                        matrix[i][i] = 'O';
                        return;
                    }
                }
            } else if (coordinates[0] == -1 && coordinates[1] == 1) { // show position is on diagonal right -> left
                for (int i = matrix.length - 1; i >= 0; i--) {
                    if (matrix[i][i] == '•') {
                        matrix[i][i] = 'O';
                        return;
                    }
                }
            } else { // position is horizontal or vertical in coordinates are directly correct coordinates.
                matrix[coordinates[0]][coordinates[1]] = 'O';
            }
        }
    }

    private static int[] checkSteps(char[][] matrix) {

        int diagonalCheckLeft = winCondition(matrix); // counter for diagonal check left -> right for 'X'
        int diagonalCheckRight = diagonalCheckLeft; // counter for diagonal check right -> left for 'X'
        int checkNeutralDiagonalLeft = 1; // counter for neutral point '•' for diagonal left -> right
        int checkNeutralDiagonalRight = 1; // counter for neutral point '•' for diagonal right -> left

        // init coordinates if they are not changed pc will insert random place
        int[] coordinatesG = {-1, -1};  // Horizontal and Diagonal
        int[] coordinatesV = {-1, -1}; // Vertical


        for (int i = 0; i < matrix.length; i++) {
            int horizontalCheck = diagonalCheckRight; // counter horizontal check for X
            int verticalCheck = horizontalCheck; // counter vertical check for Y
            int checkNeutralHorizontal = 1; // counter neutral horizontal point
            int checkNeutralVertical = 1; // counter neutral vertical point


            if (matrix[i][i] == 'X') { // check diagonal left -> right
                diagonalCheckLeft --;
            } else if (matrix[i][i] == '•') { // check diagonal left -> right for neutral point
                checkNeutralDiagonalLeft--;
            } else {
                diagonalCheckLeft ++; // means there is a 'O' it's mean match streak is broke
            }

            // The same condition like diagonal left -> right but for right -> left
            if (matrix[i][matrix.length - (i + 1)] == 'X') {
                diagonalCheckRight --;
            } else if (matrix[i][matrix.length - (i + 1)] == '•') {
                checkNeutralDiagonalRight --;
            } else {
                diagonalCheckRight ++;
            }

            for (int j = 0; j < matrix.length; j++) {
                // I check Vertical and Horizontal in one loop directly to reduce time and redundant
                if (matrix[i][j] == 'X') { // horizontal check
                    horizontalCheck --;
                } else if (matrix[i][j] == '•') {
                    checkNeutralHorizontal --;
                    coordinatesG[0] = i; // insert coordinates
                    coordinatesG[1] = j; // insert coordinates
                } else {
                    horizontalCheck ++; // streak broke
                }

                if (matrix[j][i] == 'X') { // Vertical check the same like horizontal
                    verticalCheck --;
                } else if (matrix[j][i] == '•') {
                    checkNeutralVertical --;
                    coordinatesV[0] = j;
                    coordinatesV[1] = i;
                } else {
                    verticalCheck ++;
                }

                if (horizontalCheck == 0 && checkNeutralHorizontal == 0) { // Control if horizontalCheck is 0 means on line [i] are 2 times X
                    // if checkNeutralHorizontal is 0 means on position [i][j] are '•'
                    // both this condition show where need to place 'X'
                    return coordinatesG;
                }
                if (verticalCheck == 0 && checkNeutralVertical == 0) { // the same for vertical
                    return coordinatesV;
                }
            }
            // The same check for diagonals
            if (diagonalCheckLeft == 0 && checkNeutralDiagonalLeft < 1) {
                return new int[]{-1, 0}; // -1 and 0 show that [0] = -1 means diagonal and [1] = 0 shows left -> right
            }
            if (diagonalCheckRight == 0 && checkNeutralDiagonalRight < 1) {
                return new int[]{-1, 1}; // -1 and 0 show that [0] = -1 means diagonal [1] = 1 shows right -> left
            }
        }
        return new int[]{-1,-1}; // it's not danger situation
    }

    private static int[] humanStep(char[][] matrix) {
        /**
         * Human step
         * @param matrix is actual matrix
         * @return int array with data
         * */
        System.out.println("Insert your coordinates:");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        boolean correctInput = true;
        int minCoordinate = 1;
        int maxCoordinate = matrix.length;
        int coordinateX;
        int coordinateY;
        int[] data = new int[2];
        while (correctInput) {
            try {
                int coordinate = Integer.parseInt(reader.readLine());
                coordinateX = coordinate / 10; // coordinate x example: 21 / 10 mean that x will be 2
                coordinateY = coordinate % 10; // coordinate y example: 21 % 10 mean that y will be 1

                correctInput = coordinatesChecks(coordinateX, coordinateY, minCoordinate, maxCoordinate);
                if (correctInput) {
                    System.out.println("Insert correct coordinates!");
                } else if (matrix[coordinateX - 1][coordinateY - 1] == '•') {
                    matrix[coordinateX - 1][coordinateY - 1] = 'X';
                    data[0] = coordinateX - 1;
                    data[1] = coordinateY - 1;
                } else {
                    correctInput = true;
                    System.out.println("This place is busy!");
                }
            } catch (NumberFormatException | IOException e) {
                System.out.println("Insert correct coordinates!");
            }
        }
        reader = null;
        return data;
    }

    private static boolean coordinatesChecks(int x, int y, int min, int max) {
        /**
         *  Control human coordinates to catch IndexOutOfBounds
         *  */
        return !(x >= min && x <= max && y >= min && y <= max);
    }

    private static int winCondition(char[][] matrix) {
        /**
         *  Control game condition 3x3 or 5x5 it's important for win and pc calculation
         *  */
        if (matrix.length == 3) {
            return 2;
        }
        return 3;
    }

    private static void checkCorners(char[][] matrix) {
        /**
         *  Check corners to place there O
         *  */
        Random random = new Random();
        int x = random.nextInt(matrix.length);
        int[][] possibilities = new int[4][2];
        possibilities[0] = new int[]{0, 0};
        possibilities[1] = new int[]{0, matrix.length - 1};
        possibilities[2] = new int[]{matrix.length - 1, 0};
        possibilities[3] = new int[]{matrix.length, matrix.length};
        matrix[possibilities[x][0]][possibilities[x][1]] = 'O';
    }

    private static int getSize() {
        /**
         * Matrix size check to catch another possibilities, it will work only with 3 and 5!
         * */
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        int size = 0;
        while (size != 3 && size != 5) {
            try {
                size = Integer.parseInt(reader.readLine());
                if (size != 3 && size != 5) {
                    failMessage();
                }
            } catch (NumberFormatException | IOException e) {
                failMessage();
            }
        }
        reader = null;
        System.out.println("Thx matrix size is " + size + " x " + size);
        return size;
    }

    private static char[][] initGamePlace(int x) {
        /**
         * Init matrix with '•'
         * @param x = "size of matrix can be only 3x3 or 5x5 "
         * @return 3x3 or 5x5 matrix with '•' initialized
         * */
        char[][] gamePlace = new char[x][x];
        for (int i = 0; i < gamePlace.length; i++) {
            for (int j = 0; j < gamePlace.length; j++) {
                gamePlace[i][j] = '•';
            }
        }
        return gamePlace;
    }

    private static void printMatrix(char[][] matrix) {
        /**
         * Print matrix
         * @param matrix = Matrix which will be print
         * */
        System.out.println("Matrix looks like this");
        for (int i = 0; i < matrix.length; i++) {
            for (int j = 0; j < matrix.length; j++) {
                System.out.print(matrix[i][j] + " ");
            }
            System.out.println();
        }
    }

    private static void failMessage() {
        /**
         * Fail massage to catch errors
         * */
        System.out.println("Enter please number 3 or number 5!");
    }

    private static short controlGame(char[][] matrix) {
        /**
         * Control if someone win the game
         * @param matrix = updated matrix which need to be verified
         * */

        int[] results = controlXAndY(winCondition(matrix), matrix);
        if (results.length == 1) {
            if (results[0] == 0) {
                System.out.println("Win friendship");
                return 0;
            } else {
                return 1;
            }
        } else {
            return 0;
        }
    }

    private static int[] controlXAndY(int condition, char[][] matrix) {
        /**
         * This Method control if someone win or not
         * @param condition is condition how big is matrix 3x3 or 5x5, win condition for 3x3 are 3 matches, for 5x5 are 4 matches
         * @oaram matrix is updated matrix which will be controlled
         * @return array with data
         * */
        int finish = 0;

        int diagonalConditionL = condition + 1;
        int diagonalConditionLY = condition + 1;

        int diagonalConditionR = condition + 1;
        int diagonalConditionRY = condition + 1;

        char initX = 'X';
        char initY = 'O';

        for (int i = 0; i < matrix.length; i++) {
            int intermediateConditionX = condition + 1;
            int intermediateConditionXY = condition + 1;

            int intermediateConditionYX = condition + 1;
            int intermediateConditionY = condition + 1;

            if (matrix[i][i] == '•' || matrix[i][matrix.length - (i + 1)] == '•') {
                finish = 1;
            }

            if (matrix[i][i] == initX) {
                diagonalConditionL--;
            } else if ( matrix[i][i] == initY) {
                diagonalConditionLY --;
            }

            if (matrix[i][matrix.length - (i + 1)] == initX) {
                diagonalConditionR--;
            } else if (matrix[i][matrix.length - (i + 1)] == initY) {
                diagonalConditionRY--;
            }

            for (int j = 0; j < matrix.length; j++) {
                if (matrix[i][j] == '•' || matrix[j][i] == '•') {
                    finish = 1;
                }

                if (matrix[i][j] == initX) {
                    intermediateConditionX--;
                } else if (matrix[i][j] == initY) {
                    intermediateConditionXY--;
                }
                if (matrix[j][i] == initY) {
                    intermediateConditionYX--;
                } else if (matrix[j][i] == initX) {
                    intermediateConditionY--;
                }

            }
            if (controlCondition(intermediateConditionX, intermediateConditionXY, intermediateConditionYX, intermediateConditionY)) {
                return new int[]{0, 1};
            }
        }
        if (controlCondition(diagonalConditionL, diagonalConditionLY, diagonalConditionR, diagonalConditionRY)) {
            return new int[]{0, 1};
        }
        return new int[]{finish};
    }

    private static boolean controlCondition(int diagonalConditionL, int diagonalConditionLY, int diagonalConditionR, int diagonalConditionRY) {
        /**
         * Control conditions who win Human or Computer, else no one!
         * */
        if (diagonalConditionL == 0 || diagonalConditionR == 0) {
            System.out.println("You win");
            return true;
        }
        if (diagonalConditionLY == 0 || diagonalConditionRY == 0) {
            System.out.println("Win computer");
            return true;
        }
        return false;
    }

}