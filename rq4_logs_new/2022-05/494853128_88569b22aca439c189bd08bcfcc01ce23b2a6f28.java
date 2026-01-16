package ru.geekbrains.january_chat.chat_server.Lesson4;

public class WaitNotifyClass {

        private final Object mon = new Object();
        private String currentLetter = "A";


        public static void main(String[] args) {
            WaitNotifyClass waitNotifyObj = new WaitNotifyClass();

            Thread thread1 = new Thread(new Runnable() {

                @Override
                public void run() {
                    waitNotifyObj.printA();
                }
            });


            Thread thread2 = new Thread(new Runnable() {

                @Override
                public void run() {
                    waitNotifyObj.printB();
                }
            });

            Thread thread3 = new Thread(new Runnable() {

                @Override
                public void run() {
                    waitNotifyObj.printC();
                }
            });


            thread1.start();
            thread2.start();
            thread3.start();


        }



        public void printA() {
            synchronized (mon) {
                try {
                    for (int i = 0; i < 5; i++) {
                        while (currentLetter != "A") {
                            mon.wait();
                        } System.out.print("A");
                        currentLetter = "B";
                        mon.notifyAll();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }



        public void printB() {
            synchronized (mon) {
                try {
                    for (int i = 0; i < 5; i++) {
                        while (currentLetter != "B") {
                            mon.wait();
                        }
                        System.out.print("B");
                        currentLetter = "C";
                        mon.notifyAll();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }



        public void printC() {
            synchronized (mon) {
                try {
                    for (int i = 0; i < 5; i++) {
                        while (currentLetter != "C") {
                            mon.wait();
                        }
                        System.out.print("C");
                        currentLetter = "A";
                        mon.notifyAll();
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }






}