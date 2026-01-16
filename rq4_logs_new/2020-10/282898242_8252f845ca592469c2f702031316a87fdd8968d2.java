package ru.geekbrains.java_three.lesson_four;

public class klass2 {
    private final Object lock = new Object();
    private char currentLetter = 'A';

    public static void main(String[] args) {


        klass2 w = new klass2();
        Thread thread = new Thread(()->{
            w.printA();
        });
        Thread thread1 = new Thread(() -> {
            w.printB();
        });
        Thread thread2 = new Thread(() -> {
            w.printC();
        });
        thread.start();
        thread1.start();
        thread2.start();
    }

    public void printA() {
        synchronized (lock) {
            try {
                for (int i = 0; i < 5; i++) {
                    while (currentLetter != 'A') {
                        lock.wait();
                    }
                    System.out.print("A");
                    currentLetter = 'B';
                    lock.notifyAll();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    public void printB() {
        synchronized (lock) {
            try {
                for (int i = 0; i < 5; i++) {
                    while (currentLetter != 'B') {
                        lock.wait();
                    }
                    System.out.print("B");
                    currentLetter = 'C';
                    lock.notifyAll();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    public void printC() {
        synchronized (lock) {
            try {
                for (int i = 0; i < 5; i++) {
                    while (currentLetter != 'C') {
                        lock.wait();
                    }
                    System.out.print("C");
                    currentLetter = 'A';
                    lock.notifyAll();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

