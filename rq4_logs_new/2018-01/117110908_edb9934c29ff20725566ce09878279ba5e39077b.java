package Chapter8;

import java.util.*;

/**
 * Program shows array of workers timestamp
 *
 * @author Raymond Luong
 */
public class C8_4 {

    /**
     * Main Method
     *
     * @param args arguments from command line prompt
     */
    public static void main(String[] args) {
        int[] total = new int[8];
        int[][] hours
                = {
                    {2, 4, 3, 4, 5, 8, 8},
                    {7, 3, 4, 3, 3, 4, 4},
                    {3, 3, 4, 3, 3, 2, 2},
                    {9, 3, 4, 7, 3, 4, 1},
                    {3, 5, 4, 3, 6, 3, 8},
                    {3, 4, 4, 6, 3, 4, 4},
                    {3, 7, 4, 8, 3, 8, 4},
                    {6, 3, 5, 9, 2, 7, 9}
                };
        System.out.println("\n\t  SuM T W ThF Sa Total");
        for (int row = 0; row < hours.length; row++) {
            System.out.print("\nEmployee" + row + " ");
            for (int column = 0; column < hours[row].length; column++) {
                total[row] += hours[row][column];
                System.out.print(hours[row][column] + " ");
            }
            System.out.print(total[row]);
        }
    }
}