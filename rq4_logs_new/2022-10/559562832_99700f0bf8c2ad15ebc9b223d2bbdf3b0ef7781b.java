import java.io.IOException;
import java.util.*;

class Bar_Chart {
    public static void main(String args[]) throws IOException {
        Scanner scn = new Scanner(System.in);
        int i, j, n;
        n = scn.nextInt();
        int[] arr = new int[n];

        for (i = 0; i < arr.length; i++) {
            arr[i] = scn.nextInt();
        }

        int max = arr[0];

        for (i = 0; i < arr.length; i++) {
            if (arr[i] > max)
                max = arr[i];
        }

        for (i = max; i >= 1; i--) {
            for (j = 0; j < arr.length; j++) {
                if (arr[j] >= i) {
                    System.out.print("*\t");
                } else {
                    System.out.print(" \t");
                }
            }
            System.out.println();
        }
    }
}