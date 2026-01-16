package ru.chernov.java.basic.homeworks.homework14;

public class Main {
    public static void main(String[] args) {
        try {
            double100Mln(0);
            double100Mln(1);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void double100Mln(int mode) throws InterruptedException {
        double[] doubleArray = new double[100_000_000];
        if (mode == 0) {
            System.out.print("double array 100 mln items. One thread");
            long startTime = System.currentTimeMillis();
            for (int i = 0; i < doubleArray.length; i++) {
                doubleArray[i] = 1.14 * Math.cos(i) * Math.sin(i * 0.2) * Math.cos(i / 1.2);

            }
            System.out.println("-->Time spent " + (System.currentTimeMillis() - startTime) + " ms");
        }
        if (mode == 1) {
            System.out.print("double array 100 mln items. Four threads");
            long startTime = System.currentTimeMillis();
            Thread th1 = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < 25_000_000; i++) {
                        doubleArray[i] = 1.14 * Math.cos(i) * Math.sin(i * 0.2) * Math.cos(i / 1.2);
                    }
                }
            });
            Thread th2 = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int i = 25_000_000; i < 50_000_000; i++) {
                        doubleArray[i] = 1.14 * Math.cos(i) * Math.sin(i * 0.2) * Math.cos(i / 1.2);
                    }
                }
            });
            Thread th3 = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int i = 50_000_000; i < 75_000_000; i++) {
                        doubleArray[i] = 1.14 * Math.cos(i) * Math.sin(i * 0.2) * Math.cos(i / 1.2);
                    }
                }
            });
            Thread th4 = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int i = 75_000_000; i < 100_000_000; i++) {
                        doubleArray[i] = 1.14 * Math.cos(i) * Math.sin(i * 0.2) * Math.cos(i / 1.2);
                    }
                }
            });
            th1.start();
            th2.start();
            th3.start();
            th4.start();
            th1.join();
            th2.join();
            th3.join();
            th4.join();
            System.out.println("-->Time spent " + (System.currentTimeMillis() - startTime) + " ms");
        }
    }
}