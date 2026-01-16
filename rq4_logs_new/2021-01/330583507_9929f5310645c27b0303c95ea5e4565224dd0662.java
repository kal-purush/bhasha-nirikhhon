package Temp;

import java.util.concurrent.Semaphore;

public class Main {
    public static void main(String[] args) {
        final Semaphore semaphore = new Semaphore(2);

        Runnable limitedCall = new Runnable() {
            int count = 0;
            @Override
            public void run() {
                int time = 3 + (int) (Math.random() * 4.0);
                int num = count++;
            try {
                semaphore.acquire();
                System.out.println("Поток #" + num + " начинает выполнять очень долгое действие " + time + " сек.");
                Thread.sleep(time * 1000);
                System.out.println("Поток #" + num + " завершил работу!");
            }catch (InterruptedException e){
                e.printStackTrace();
            }finally {
                semaphore.release();
            }
            }

        };

        for (int i = 0; i < 10; i++) {
            new Thread(limitedCall).start();
        }
    }
}