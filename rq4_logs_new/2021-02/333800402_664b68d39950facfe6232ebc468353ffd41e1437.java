package JAVA_LEVEL_2.Java_lvl2_homework.git.lesson5.hw5;

public class WithFlow {
    public static void WithFlow(float[] arr, int h){
    float[] a1 = new float[h];
    float[] a2 = new float[h];

        for (int i = 0; i < arr.length; i++) {
            arr[i] =1;
        }
        long a = System.currentTimeMillis();//начало отсчёта
        System.arraycopy(arr,0,a1,0,h);//копирование половины массива в а1
        System.arraycopy(arr,h,a2,0,h);//копирование другой половины массива в а2
        Thread thread1 = new Thread(new Runnable() {//создаю первый поток
            @Override
            public void run() {
                for (int i = 0; i < a1.length; i++) {
                    a1[i] = (float)(a1[i] * Math.sin(0.2f + i / 5) * Math.cos(0.2f + i / 5) * Math.cos(0.4f + i / 2));
                    //System.out.print(a1[i] + "\t");
                }
            }
        });
        Thread thread2 = new Thread(new Runnable() {//создаю второй поток
            @Override
            public void run() {
                for (int i = 0; i < a2.length; i++) {
                    a2[i] = (float)(a2[i] * Math.sin(0.2f + (i+a2.length) / 5) * Math.cos(0.2f + (i+a2.length)/ 5) * Math.cos(0.4f + (i+a2.length) / 2));
                    //System.out.print(a2[i] + "\t");
                }
            }
        });
        thread1.start();
        thread2.start();
        try {
            thread1.join();
            thread2.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.arraycopy(a1, 0, arr, 0, WorkingTime.h);
        System.arraycopy(a2, 0, arr, WorkingTime.h, WorkingTime.h);
//        float sum1 =0;
//        for (int i = 0; i < arr.length; i++) {
//            sum1 = sum1 + arr[i];
//        }
        System.currentTimeMillis();//конец отсчёта
//        System.out.println(sum1);
        System.out.println("Время работы метода с потоками: " + (System.currentTimeMillis() - a) + " миллиcекунд.");
    }
}