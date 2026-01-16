package com.cropmy.vladyslav.cropandroid;

public class EncodeUtils {
    private EncodeUtils() {

    }

    public static int encodeMargins(int countPixels, int margins[]) {
        if (margins == null || margins.length != 4) {
            return -1;
        }
        int res = 0;
        int multiplier = (countPixels + 1);
        for (int i = 0; i < margins.length; i++) {
            res += multiplier * margins[i];
            multiplier *= (countPixels + 1);
        }
        return res;
    }

    public static int[] decodeMargins(int countPixels, int value) {
        int newCount = countPixels + 1;
        int maxMultiplier = pow(newCount, 5);
        if (value < 0 || value >= maxMultiplier) {
            return null;
        }
        maxMultiplier /= newCount;
        int[] res = new int[4];

        for (int i = res.length - 1; i >= 0; i--) {
            res[i] = value / maxMultiplier;
            value -= res[i] * maxMultiplier;
            maxMultiplier /= newCount;
        }
        return res;
    }

    private static int pow(int a, int b) {
        if (b == 0) {
            return 1;
        } else if (b == 1) {
            return a;
        }
        int res = pow(a, b / 2);
        res *= res;
        if (b % 2 == 1) {
            res *= a;
        }
        return res;
    }
}