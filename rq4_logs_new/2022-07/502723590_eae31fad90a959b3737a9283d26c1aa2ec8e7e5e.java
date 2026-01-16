package File_work.Branch;


class selectionSortThread extends Thread {
    private final int[] AS;
    private final Thread t;

    public selectionSortThread(int[] AS, String threadName) {
        this.AS = AS;
        t = new Thread(this, threadName);
    }

    @Override
    public void start() {
        t.start();
    }

    @Override
    public void run() {
        System.out.println("Begin -> " + t.getName());
        int i, j, k;
        int s;

        for (i = 0; i < AS.length - 1; i++) {
            k = i;
            s = AS[i];
            for (j = i + 1; j < AS.length; j++) {
                if (AS[j] > s) {
                    k = j;
                    s = AS[j];
                }
            }
            AS[k] = AS[i];
            AS[i] = s;
            PrintArr.Print(AS, t.getName());
        }
        System.out.println("End -> " + t.getName());
    }

    public int[] get() {
        return AS;
    }

    public Thread getThread() {
        return t;
    }

}

class InsertionSortThread extends Thread {
    private final int[] AS;
    private final Thread t;

    public InsertionSortThread(int[] AS, String threadName) {
        this.AS = AS;
        t = new Thread(this, threadName);
    }

    @Override
    public void start() {
        t.start();
    }

    @Override
    public void run() {
        System.out.println("Begin -> " + t.getName());
        int i, j;
        int s;

        for (i = 0; i < AS.length; i++) {
            s = AS[i];
            for (j = i - 1; j >= 0 && AS[j] < s; j--) {
                AS[j + 1] = AS[j];
            }
            AS[j + 1] = s;
            PrintArr.Print(AS, t.getName());
        }
        System.out.println("End -> " + t.getName());
    }

    public int[] get() {
        return AS;
    }

    public Thread getThread() {
        return t;
    }

}

class BubbleSortThread extends Thread {
    private final int[] AS;
    private final Thread t;
    public BubbleSortThread(int[] AS, String threadName) {
        this.AS = AS;
        t = new Thread(this, threadName);
    }

    @Override
    public void start() {
        t.start();
    }

    @Override
    public void run() {
        System.out.println("Begin -> " + t.getName());
        int i, j;
        int s;
        for (i = 0; i < AS.length; i++) {
            for (j = AS.length - 1; j > i; j--) {
                if (AS[j - 1] < AS[j]) {
                    s = AS[j];
                    AS[j] = AS[j - 1];
                    AS[j - 1] = s;
                    PrintArr.Print(AS, t.getName());
                }
            }
        }
        System.out.println("End -> " + t.getName());
    }

    public int[] get() {
        return AS;
    }

    public Thread getThread() {
        return t;
    }
}

class PrintArr {
    synchronized public static void Print(int[] arr, String name) {
        System.out.print(name + " = [ ");
        for (int i : arr) {
            System.out.print(i + " ");
        }
        System.out.println("]");
    }
}

public class task_2 {
    public static void main(String[] args) {
        int[] ar1 = {1, 6, 2, 8, 5, 4, 9, 10};
        int[] ar2 = {1, 6, 2, 8, 5, 4, 9, 10};
        int[] ar3 = {1, 6, 2, 8, 5, 4, 9, 10};
        selectionSortThread t1 = new selectionSortThread(ar1, "t1:SelectionSort");
        InsertionSortThread t2 = new InsertionSortThread(ar2, "t2:InsertionSort");
        BubbleSortThread t3 = new BubbleSortThread(ar3, "t3:BubbleSort");

        t1.start();
        t2.start();
        t3.start();

        try {
            t1.getThread().join();
            t2.getThread().join();
            t3.getThread().join();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }

        PrintArr.Print(ar1, t1.getThread().getName());
        PrintArr.Print(ar2, t2.getThread().getName());
        PrintArr.Print(ar3, t3.getThread().getName());
    }
}