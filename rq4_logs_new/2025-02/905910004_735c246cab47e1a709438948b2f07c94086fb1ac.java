package lesson_12_Multithreading.thread_state_examples;

public class BlockedState {
    public static void main(String[] args) throws InterruptedException {
        Thread t1 = new Thread(new DemoBlockedState());
        Thread t2 = new Thread(new DemoBlockedState());

        t1.start();
        t2.start();

        Thread.sleep(1000);

        System.out.println(t2.getState());
        System.exit(0);
    }
}

class DemoBlockedState implements Runnable {

    @Override
    public void run() {
        commonResource();
    }

    public static synchronized void commonResource() {
        while(true) {

        }
    }
}




