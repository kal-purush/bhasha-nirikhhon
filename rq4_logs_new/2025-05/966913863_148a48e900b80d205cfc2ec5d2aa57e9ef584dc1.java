package basic1;

public class MergeSorterList {
    public static int[] merge(int[] a, int[] b, int[] res) {
        int i = 0;
        int j = 0;
        int k = 0;
        while (i < a.length && j < b.length) {
            if (a[i] < b[j]) {
                res[k] = a[i];
                k++;
                i++;
            } else {
                res[k] = b[j];
                k++;
                j++;
            }
        }
        while (i < a.length) {
            res[k] = a[i];
            k++;
            i++;
        }
        while (j < b.length) {
            res[k] = b[j];
            k++;
            j++;
        }
        return res;
    }

    public static void main(String[] args) {
        int[] a = {1, 2, 4, 6,9};
        int[] b = {3, 5, 7};
        int[] res = new int[a.length + b.length];
        merge(a, b, res);

        for (int item : res) {
            System.out.print(" " + item);
        }
    }
}