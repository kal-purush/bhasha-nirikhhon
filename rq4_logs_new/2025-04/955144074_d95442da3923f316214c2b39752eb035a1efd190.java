package Aiperi.array1;

public class MiddleWay {
    public int[] middleWay(int[] a, int[] b) {
        int[] result = new int[2];
        if (a.length < 3 || b.length < 3 && a.length > 3 || b.length > 3) {
            return a;
        }
        for (int i = 0; i < a.length - 1; i++) {
            if (i > 0 && i < a.length - 1) {
                result[0] = a[i];
            }
        }
        for (int i = 0; i < b.length - 1; i++) {
            if (i > 0 && i < b.length - 1) {
                result[1] = b[i];
            }
        }
        return result;
    }
}