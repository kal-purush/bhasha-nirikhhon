package lesson_13_Extended_Synchronization.livecoding.wait_notify;

public class Notifier implements Runnable {
    private Message message;

    public Notifier(Message message) {
        this.message = message;
    }

    @Override
    public void run() {
        String name = Thread.currentThread().getName();
        System.out.println(name + " started");
        try {
            Thread.sleep(1000);
            synchronized (message) {
                System.out.println(name + " notify time: " + System.currentTimeMillis());
                //message.notify(); // Будит один поток, переводя его из состояния Non-Runnable в Runnable
                message.notifyAll(); // Будит все потоки, , переводя их из состояния Non-Runnable в Runnable
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}