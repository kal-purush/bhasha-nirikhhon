package File_work.Branch;


public class task_1 {
    private static int[] arr = new int[0];
    static Thread t1, t2;
    static int m, p;


    static class Added implements Runnable {
        @Override
        public void run() {
            try {
                System.out.println("added started");
                int result = 0;
                for (int i : arr) {
                    result += i;
                }
                p = result;
                t2.join();
                System.out.println("added continuous");
                System.out.println((m + p) / 2);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    static class Multiply implements Runnable {
        @Override
        public void run() {
            try {
                System.out.println("multiply started");
                int result = 1;
                for (int i : arr) {
                    result *= i;
                }
                Thread.sleep(100);
                m = result;
                System.out.println("multiply ended");
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    public static void main(String[] args) {
        t1 = new Thread(new Added());
        t2 = new Thread(new Multiply());
        arr = new int[]{1, 2, 3, 4, 5};
        t1.start();
        t2.start();
    }




}