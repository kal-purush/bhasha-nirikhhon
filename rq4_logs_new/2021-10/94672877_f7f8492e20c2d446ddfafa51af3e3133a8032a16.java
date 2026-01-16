package com.kuldeep.algorithims;

import java.util.Arrays;

/**
 * @author <a href="kuldeepyadav7291@gmail.com">Kuldeep</a>
 */
class RotateImageUtil {
    private RotateImageUtil() { throw new UnsupportedOperationException(); }

    public static int[][] rotate(int[][] image) {
        int n = image.length;

        int[][] original = Arrays.copyOf(image, n);

        for (int i = 0; i < n / 2; i++) {
            for (int j = i; j < n - i - 1; j++ ) {
                int temp1 = original[i][j];
                int temp2 = original[j][n - i -1];
                int temp3 = original[n - i - 1][n - j - 1];
                int temp4 = original[n - j - 1][i];

                original[i][j] = temp4;
                original[j][n-i-1] = temp1;
                original[n-i-1][n-j-1] = temp2;
                original[n-j-1][i] = temp3;
            }
        }

        return original;
    }
}