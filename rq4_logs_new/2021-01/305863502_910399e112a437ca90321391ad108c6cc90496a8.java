package Level3.waitingQueue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class WaitingPrinter {

    private static final int COUNT = 5;
    private static final char NO_CHAR = 0;

    private volatile char lastPrinted = NO_CHAR;

    public static void main(String[] args) {
        WaitingPrinter printer = new WaitingPrinter();
        new Thread(new PrintingInstance(printer,'A', 'C', true)).start();
        new Thread(new PrintingInstance(printer,'B', 'A')).start();
        new Thread(new PrintingInstance(printer,'C', 'B')).start();
    }

    private synchronized void printOne(char literal, char trigger, boolean triggerOnNoChar) {
        while (lastPrinted != trigger && (!triggerOnNoChar || lastPrinted != NO_CHAR)) {
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();
                return;
            }
        }

        System.out.print(literal);
        lastPrinted = literal;
        notifyAll();
    }

    static private class PrintingInstance implements Runnable {
        private WaitingPrinter owner;
        private final char literal;
        private final char trigger;
        private final boolean triggerOnNoChar;

        public PrintingInstance(WaitingPrinter owner, char literal, char trigger) {
            this(owner, literal, trigger, false);
        }

        public PrintingInstance(WaitingPrinter owner, char literal, char trigger, boolean triggerOnNoChar) {
            this.owner = owner;
            this.literal = literal;
            this.trigger = trigger;
            this.triggerOnNoChar = triggerOnNoChar;
        }

        @Override
        public void run() {
            for (int i = 1; i <= COUNT; i++) owner.printOne(literal, trigger, triggerOnNoChar);
        }
    }
}