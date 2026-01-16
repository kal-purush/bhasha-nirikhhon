package lambdasinaction.ahandful;

import java.sql.Time;
import java.time.LocalDateTime;

public class Daemon {
    public static void main(String[] args) throws InterruptedException {
        long l = System.nanoTime();
        Thread thread = new Thread(() -> {
            try {
                Thread.sleep(1000);
                System.out.println("haha");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        Thread thread1 = new Thread(() -> {
            try {
                Thread.sleep(1000);
                System.out.println("haha");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        System.out.print("线程未启动");
        System.out.println((System.nanoTime()-l)/1_000_000);
        thread.setDaemon(true);
        thread.start();
        System.out.print("线程启动");
        System.out.println((System.nanoTime()-l)/1_000_000);
//        Thread.sleep(1000);
        System.out.print("休眠结束");
        System.out.println((System.nanoTime()-l)/1_000_000);
        thread1.setDaemon(true);
        thread1.start();
    }
}