import java.util.concurrent.Semaphore;
import java.util.Random;

public class RW {
    static int buf = 0; // Data
    static int readCount = 0;
    static Semaphore mutex = new Semaphore(1);
    static Semaphore wrt = new Semaphore(1);
    static Semaphore readerSem = new Semaphore(1);
    static int numReaders;
    static int numWriters;
    static int numAccesses;

    public static void main(String[] args) {
        // Welcome message and instructions
        System.out.println("Welcome to the Reader-Writer Synchronization Program!");
        System.out.println(
                "Please provide the number of readers, number of writers, and the number of accesses each should perform.");
        System.out.println(
                "Number of Readers (<# of readers>): Number of reader threads to create. Each will attempt to read from the shared buffer.");
        System.out.println(
                "Number of Writers (<# of writers>): Number of writer threads to create. Each will attempt to write to the shared buffer.");
        System.out.println(
                "Number of Accesses (<# of access>): Number of times each thread (both readers and writers) will access the shared buffer.\n");
        if (args.length != 3) {
            System.out.println("Usage: java RW <# of readers> <# of writers> <# of access>");
            System.exit(1);
        }

        numReaders = Integer.parseInt(args[0]);
        numWriters = Integer.parseInt(args[1]);
        numAccesses = Integer.parseInt(args[2]);

        System.out.println("# of readers is: " + numReaders);
        System.out.println("# of writers is: " + numWriters);

        Thread[] readerThreads = new Thread[numReaders];
        Thread[] writerThreads = new Thread[numWriters];

        for (int i = 0; i < numReaders; i++) {
            readerThreads[i] = new Thread(new Reader("Reader " + (char) ('A' + i)));
            readerThreads[i].start();
        }

        for (int i = 0; i < numWriters; i++) {
            writerThreads[i] = new Thread(new Writer("Writer " + (char) ('F' + i)));
            writerThreads[i].start();
        }
    }

    static class Reader implements Runnable {
        private String name;
        private Random rand = new Random();

        public Reader(String name) {
            this.name = name;
        }

        public void run() {
            for (int i = 0; i < numAccesses; i++) {
                try {
                    // Entry Section
                    readerSem.acquire();
                    mutex.acquire();
                    readCount++;
                    if (readCount == 1) {
                        wrt.acquire(); // Writers have priority
                    }
                    mutex.release();
                    readerSem.release();

                    // Reading the data
                    System.out.println(name + " retrieved " + buf);

                    // Exit Section
                    mutex.acquire();
                    readCount--;
                    if (readCount == 0) {
                        wrt.release(); // Allow a waiting writer to enter
                    }
                    mutex.release();

                    // Simulate a real situation with sleep
                    Thread.sleep(rand.nextInt(1001));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    static class Writer implements Runnable {
        private String name;
        private Random rand = new Random();

        public Writer(String name) {
            this.name = name;
        }

        public void run() {
            for (int i = 0; i < numAccesses; i++) {
                try {
                    // Entry Section
                    wrt.acquire();

                    // Writing the data
                    int newData = rand.nextInt(10);
                    buf = newData;
                    System.out.println(name + " set buffer to " + newData);

                    // Exit Section
                    wrt.release();

                    // Simulate a real situation with sleep
                    Thread.sleep(rand.nextInt(1001));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}