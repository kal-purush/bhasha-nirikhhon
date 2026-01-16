package lesson5;

public class Main5 {
    public static void main(String[] args) {
        final int size = 10000000;
        final int h = size / 2;
        float[] arr = new float[size];

        method1();
        method2();


    }

    public static void method1() {
        final int size = 10000000;
        final int h = size / 2;
        float[] arr = new float[size];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = 1.0f;
        }

        long a = System.currentTimeMillis();

        for (int i = 0; i < arr.length; i++) {
            arr[i] = (float) (arr[i] * Math.sin(0.2f + i / 5) * Math.cos(0.2f + i / 5) * Math.cos(0.4f + i / 2));
        }

        System.currentTimeMillis();
        System.out.println("Время на первый метод: " + (System.currentTimeMillis() - a));
    }


    public static void method2() {

        final int size = 10000000;
        final int h = size / 2;
        float[] arr = new float[size];

        for (int i = 0; i < arr.length; i++) {
            arr[i] = 1.0f;
        }

        float[] a1 = new float[h];
        float[] a2 = new float[h];

        long a = System.currentTimeMillis();
        System.arraycopy(arr, 0, a1, 0, h);
        System.arraycopy(arr, h, a2, 0, h);
        System.currentTimeMillis();
        System.out.println("Разбивка на два диапазона: " + (System.currentTimeMillis() - a));

        long b = System.currentTimeMillis();
        Thread thread1 = new Thread(() -> {
            for (int i = 0; i < a1.length; i++) {
                a1[i] = (float) (a1[i] * Math.sin(0.2f + i / 5) * Math.cos(0.2f + i / 5) * Math.cos(0.4f + i / 2));
            }
        });
        System.currentTimeMillis();
        System.out.println("Просчет первого диапазона: " + (System.currentTimeMillis() - b));

        long c = System.currentTimeMillis();
        Thread thread2 = new Thread(() -> {
            for (int i = 0; i < a2.length; i++) {
                a2[i] = (float) (a2[i] * Math.sin(0.2f + i / 5) * Math.cos(0.2f + i / 5) * Math.cos(0.4f + i / 2));
            }
        });
        System.currentTimeMillis();
        System.out.println("Просчет второго диапазона: " + (System.currentTimeMillis() - c));

        long d = System.currentTimeMillis();
        System.arraycopy(a1, 0, arr, 0, h);
        System.arraycopy(a2, 0, arr, h, h);
        System.currentTimeMillis();
        System.out.println("Склейка диапазонов: "+ (System.currentTimeMillis()-d));
        System.out.println("Время на второй метод: " + (System.currentTimeMillis()-a));

        thread1.start();
        thread2.start();
    }
}
