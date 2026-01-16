package HW_01_02_02_Queue;

import java.util.concurrent.BlockingQueue;

/**
 * Created by Dmytro on 28.06.2017.
 */
public class Consumer implements Runnable {
    BlockingQueue queue;

    Consumer(BlockingQueue queue){
        this.queue = queue;
    }

    @Override
    public void run() {
        while (true) {
            try {
                queue.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}