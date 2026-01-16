public class Multithreading {

    static final int size = 10000000;
    static final int h = size / 2;

    public static void main(String[] args) {
        float[] arr = new float[size];
        for (int i = 0; i < size; i++) arr[i] = 1;

        methodOne(arr);
        methodTwo(arr);
    }

    public static void methodOne(float[] arr) {
        long a = System.currentTimeMillis();
        for (int i = 0; i < size; i++) {
            arr[i] = formula(arr[i], i);
        }
        System.out.println(System.currentTimeMillis() - a);
    }

    public static void methodTwo(float[] arr) {
        float[] arrA = new float[h];
        float[] arrB = new float[h];

        long a = System.currentTimeMillis();
        System.arraycopy(arr, 0, arrA, 0, h);
        System.arraycopy(arr, h, arrB, 0, h);

        Thread myThread1 = new Thread(new MyThread(arrA));
        Thread myThread2 = new Thread(new MyThread(arrB));
        myThread1.start();
        myThread2.start();

        try {
            myThread1.join();
            myThread2.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //либо использовать isAlive() для обоих потоков через while, но гугл сказал, что этот способ этичнее
        System.arraycopy(arrA, 0, arr, 0, h);
        System.arraycopy(arrB, 0, arr, h, h);
        System.out.println(System.currentTimeMillis() - a);

    }

    public static float formula(float elem, int i) {
        return (float) (elem * Math.sin(0.2f + i / 5) * Math.cos(0.2f + i / 5) * Math.cos(0.4f + i / 2));
    }

    public static class MyThread implements Runnable{
        private final float[] arr;
        public MyThread(float[] arr){
            this.arr = arr;
        }
        @Override
        public void run() {
            for (int i = 0; i < h; i++) {
                arr[i] = formula(arr[i], i);
            }
        }
    }
}