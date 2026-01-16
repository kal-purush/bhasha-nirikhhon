package GeekBrians.Slava_5655380.Homework.Lesson12;

import static GeekBrians.Slava_5655380.Utils.*;

import java.util.Arrays;

public class Lesson12 {
    static final int SIZE = 10_000_000;
    static final int HALF = SIZE / 2;

    public static void main(String[] args) throws InterruptedException {
        float[] arr = new float[SIZE];

        Arrays.fill(arr, 1);
        long a = System.currentTimeMillis();
        singleThreadMethod(arr);
        System.out.println(System.currentTimeMillis() - a);

        Arrays.fill(arr, 1);
        a = System.currentTimeMillis();
        multiThreadMethod(arr);
        System.out.println(System.currentTimeMillis() - a);
    }

    public static void singleThreadMethod(float[] arr) {
        modify(arr);
    }

    public static void multiThreadMethod(float[] arr) throws InterruptedException {
        Thread firstThread = new Thread(() -> {
            float[] firstHalf = new float[HALF];
            System.arraycopy(arr, 0, firstHalf, 0, HALF);
            modify(firstHalf);
            System.arraycopy(firstHalf, 0, arr, 0, HALF);
        });
        Thread secondThread = new Thread(() -> {
            float[] secondHalf = new float[HALF];
            System.arraycopy(arr, HALF, secondHalf, 0, HALF);
            modify(secondHalf);
            System.arraycopy(secondHalf, 0, arr, HALF, HALF);
        });
        firstThread.start();
        secondThread.start();
        firstThread.join();
        secondThread.join();
    }

    public static void modify(float[] arr) {
        for (var i : range(arr.length))
            arr[i] = (float) (arr[i] * Math.sin(0.2f + i / 5) * Math.cos(0.2f + i / 5) * Math.cos(0.4f + i / 2));
    }
}