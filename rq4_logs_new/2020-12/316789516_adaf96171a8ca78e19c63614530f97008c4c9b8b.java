package hw5;

import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        final int size = 10000000;
        float[] arr = new float[size];
        Arrays.fill(arr, 1);

        long time1 = method1(arr);
        long time2 = method2(arr);

        System.out.println("Method 1: " + time1 + " ms\nMethod 2: " + time2 + " ms");
    }

    public static long method1(float[] array) {
        long time = System.currentTimeMillis();

        for (int i = 0; i < array.length; i++) {
            array[i] = (float) (array[i] * Math.sin(0.2f + i / 5f) * Math.cos(0.2f + i / 5f) * Math.cos(0.4f + i / 2f));
        }

        return (System.currentTimeMillis() - time);
    }

    public static long method2(float[] array) {
        int half = array.length / 2;
        float[] arr1 = new float[half];
        float[] arr2 = new float[half];
        float[] result = new float[half * 2];

        long time = System.currentTimeMillis();
        System.arraycopy(array, 0, arr1, 0, half);
        System.arraycopy(array, half, arr2, 0, half);

        MyRunnable counter = new MyRunnable(arr2);
        counter.start();
        arr2 = counter.getArr();

        for (int i = 0; i < half; i++) {
            arr1[i] = (float) (arr1[i] * Math.sin(0.2f + i / 5f) * Math.cos(0.2f + i / 5f) * Math.cos(0.4f + i / 2f));
        }

        System.arraycopy(arr1, 0, result, 0, half);
        System.arraycopy(arr2, 0, result, half, half);

        return (System.currentTimeMillis() - time);
    }

    static class MyRunnable extends Thread {
        float[] arr;

        MyRunnable(float[] arr) {
            this.arr = arr;
        }

        @Override
        public void run() {
            for (int i = 0; i < arr.length; i++) {
                arr[i] = (float) (arr[i] * Math.sin(0.2f + i / 5f) * Math.cos(0.2f + i / 5f) * Math.cos(0.4f + i / 2f));
            }
        }

        public float[] getArr() {
            return arr;
        }
    }
}