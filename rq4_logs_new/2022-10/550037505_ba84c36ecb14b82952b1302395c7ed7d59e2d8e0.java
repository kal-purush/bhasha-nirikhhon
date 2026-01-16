import java.util.Scanner;

public class Array2DIOLogic2 {
    public static Scanner scn = new Scanner(System.in);

    public static void display(int mat[][]) {
        System.out.println("Elements are: ");
        for (int[] row : mat) {
            for (int x : row)
                System.out.print(x + " ");
            System.out.println();
        }
    }

    public static void main(String args[]) throws Exception {
        System.out.println("Enter number of rows and columns");
        int[][] arr = new int[scn.nextInt()][scn.nextInt()];

        input(arr);
        display(arr);
    }

    private static void input(int[][] arr) {
        System.out.println("Enter the array elements - row major");
        for (int[] row : arr)
            for (int ind = 0; ind < row.length; ind++)
                row[ind] = scn.nextInt();
    }
}