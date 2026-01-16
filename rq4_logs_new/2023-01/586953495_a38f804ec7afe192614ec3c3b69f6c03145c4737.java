/*
        Matrix Challenge
        Timo Karppanen
        2.11.2022
 */

import java.util.Arrays;

class Main {

    /* Works as the main method for this program */
    public static String matrixChallenge(String[] strArr) {
        String[] arraysToString = strArrToString(strArr);
        int matrix[][] = (formMatrix(arraysToString));
        System.out.println(isSquare(matrix));
        System.out.println(isSymmetric(matrix));
        return "";
    }

    /* Method returns an arraySplit, where the given String Array has been divided into pieces */
    public static String[] strArrToString(String[] strArr) {
        String commaString = String.join(",", strArr);
        String[] arraySplit = commaString.split("<>,");  /* Dividing the array to pieces from "<>," */
        return arraySplit;
    }

    /* Method forms a matrix from the given String Array. */
    public static int[][] formMatrix(String[] strArr) {

        int[][] matrix = new int[strArr.length][];      /* Declearing a 2D array  */

        for (int i = 0; i < matrix.length; i++) {
            String[] arraySplit = strArr[i].split(","); /* Dividing the arraySplit to pieces from "," */
            matrix[i] = new int[strArr.length];
            for (int j = 0; j < matrix[i].length; j++) {
                matrix[i][j] = Integer.parseInt(arraySplit[j]);
            }
        }

        return matrix;
    }

    /* Checking if the matrix is square matrix */
    public static String isSquare(int matrix[][]) {

        int rows = matrix.length;  /* Rows */
        int columns = matrix[0].length; /* Columns */

        if (rows != columns) {
            return "not possible";
        }
        else {
            return "";
        }
    }

    /* Method checks if the matrix is symmetric */
    public static String isSymmetric(int matrix[][] ){

        int row = matrix.length;
        int col = matrix[0].length;
        int i,j, flag = 1;      /* Flag defines if the matrix is symmetric or not  */

        int[][] transpose = new int[row][col];

        for ( i = 0; i < row; i++){

            for ( j = 0; j < col; j++){
                transpose[j][i] = matrix[i][j];
            }
        }
        if ( row == col){

            for ( i = 0; i < row; i++){

                for ( j = 0; j < col; j++){

                    if (matrix[i][j] != transpose[i][j]) {
                        flag = 0;
                        break;
                    }
                }
                if ( flag == 0){
                    return "not symmetric";
                }
            }
            if (flag == 1){
                return "symmetric";
            }
            else{
                return "not symmetric";
            }
        }
        return "";
    }
    /* Program starts from here */
    public static void main (String[] args) {
        System.out.print(matrixChallenge(new String[] {"1","0","1","<>","0","1","0","<>","1","0","1"}));
    }
}