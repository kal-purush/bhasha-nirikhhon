import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class HomeWorkThreads {

    private String flag = "A";
    private final Object pojo = new Object();
    private final CyclicBarrier barrier = new CyclicBarrier(3);
    private final CountDownLatch latch = new CountDownLatch(1);
    private final Lock lock = new ReentrantLock();
    private final Condition conditionA = lock.newCondition();
    private final Condition conditionB = lock.newCondition();
    private final Condition conditionC = lock.newCondition();

    public static void main(String[] args) throws InterruptedException {
        HomeWorkThreads threadControl = new HomeWorkThreads();
        ExecutorService executorService;
        for (int i = 0; i < 10; i++) {
            executorService = Executors.newFixedThreadPool(3);
            executorService.execute(threadControl::printBWithWaitNotify);
            executorService.execute(threadControl::printCWithWaitNotify);
            executorService.execute(threadControl::printAWithWaitNotify);
            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.DAYS);
        }
        System.out.println();
        for (int i = 0; i < 10; i++) {
            executorService = Executors.newFixedThreadPool(3);
            executorService.execute(threadControl::printBWithBarrier);
            executorService.execute(threadControl::printCWithBarrier);
            executorService.execute(threadControl::printAWithBarrier);
            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.DAYS);
        }
        System.out.println();
        for (int i = 0; i < 10; i++) {
            executorService = Executors.newFixedThreadPool(3);
            executorService.execute(threadControl::printCWithLatch);
            executorService.execute(threadControl::printBWithLatch);
            executorService.execute(threadControl::printAWithLatch);
            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.DAYS);
        }
        System.out.println();
        for (int i = 0; i < 10; i++) {
            executorService = Executors.newFixedThreadPool(3);
            executorService.execute(threadControl::printCWithLock);
            executorService.execute(threadControl::printBWithLock);
            executorService.execute(threadControl::printAWithLock);
            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.DAYS);
        }
    }

    public void printAWithWaitNotify() {
        synchronized (pojo) {
            try {
                for (int i = 0; i < 3; i++) {
                    while (!flag.equals("A")) {
                        pojo.wait();
                    }
                    System.out.print("A");
                    flag = "B";
                    pojo.notifyAll();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void printBWithWaitNotify() {
        synchronized (pojo) {
            try {
                for (int i = 0; i < 3; i++) {
                    while (!flag.equals("B")) {
                        pojo.wait();
                    }
                    System.out.print("B");
                    flag = "C";
                    pojo.notifyAll();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void printCWithWaitNotify() {
        synchronized (pojo) {
            try {
                for (int i = 0; i < 3; i++) {
                    while (!flag.equals("C")) {
                        pojo.wait();
                    }
                    System.out.print("C");
                    flag = "A";
                    pojo.notifyAll();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void printAWithBarrier() {
        try {
            for (int i = 0; i < 3; i++) {
                while (!flag.equals("A")) {
                    barrier.await();
                }
                System.out.print("A");
                flag = "B";
            }
            barrier.await();
        } catch (BrokenBarrierException e) {
            System.out.println("Поток А закончил работу");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void printBWithBarrier() {
        try {
            for (int i = 0; i < 3; i++) {
                while (!flag.equals("B")) {
                    barrier.await();
                }
                System.out.print("B");
                flag = "C";
            }
        } catch (InterruptedException | BrokenBarrierException e) {
            e.printStackTrace();
        }
        barrier.reset();
    }

    public void printCWithBarrier() {
        try {
            for (int i = 0; i < 3; i++) {
                while (!flag.equals("C")) {
                    barrier.await();
                }
                System.out.print("C");
                flag = "A";
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            System.out.print("C");
            flag = "A";
        }
    }

    public void printAWithLatch() {
        try {
            for (int i = 0; i < 3; i++) {
                while (!flag.equals("A")) {
                    latch.await();
                }
                System.out.print("A");
                flag = "B";
                latch.countDown();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void printBWithLatch() {
        try {
            for (int i = 0; i < 3; i++) {
                while (!flag.equals("B")) {
                    latch.await();
                }
                System.out.print("B");
                flag = "C";
                latch.countDown();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void printCWithLatch() {
        try {
            for (int i = 0; i < 3; i++) {
                while (!flag.equals("C")) {
                    latch.await();
                }
                System.out.print("C");
                flag = "A";
                latch.countDown();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void printAWithLock() {
        try {
            lock.lock();
            for (int i = 0; i < 3; i++) {
                while (!flag.equals("A")) {
                    conditionA.await();
                }
                System.out.print("A");
                flag = "B";
                conditionB.signal();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    public void printBWithLock() {
        try {
            lock.lock();
            for (int i = 0; i < 3; i++) {
                while (!flag.equals("B")) {
                    conditionB.await();
                }
                System.out.print("B");
                flag = "C";
                conditionC.signal();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    public void printCWithLock() {
        lock.lock();
        try {
            for (int i = 0; i < 3; i++) {
                while (!flag.equals("C")) {
                    conditionC.await();
                }
                System.out.print("C");
                flag = "A";
                conditionA.signal();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

}