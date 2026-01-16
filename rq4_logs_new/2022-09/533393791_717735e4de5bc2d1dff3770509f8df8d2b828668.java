package ua.lyashko.lesson5;

import java.util.Arrays;

public class Task3 {
    public static void main ( String[] args ) {
        int row = 5;
        int column = 5;
        int[][] arr = new int[row][column];
        fillArr ( arr );
        System.out.println ( Arrays.deepToString ( swapArr ( arr,row,column ) ).replace ( "], " , "]" +
                "\n" ).replace ( "[[" , "[" ).replace ( "]]" , "]" ) );
    }

    public static void fillArr ( int[][] arr ) {
        int count = 1;
        for (int i = 0; i < arr.length; i++) {
            for (int j = 0; j < arr.length; j++) {
                arr[i][j] = count;
                count++;
            }
        }
    }

    public static int[][] swapArr ( int[][] arr, int row, int column) {
        int[][] targetArr = new int[row][column];
        for (int i = 0; i < row; i++) {
            for (int j = 0; j < column; j++) {
                targetArr[j][i] = arr[i][j];
            }
        }
        return targetArr;
    }
}