package org.china.framework.spring.synchronize;

/**
 * Created by junhuiji on 2015/12/25.
 */
public class Client {
    public static void main(String[] args) {

        Example example = new Example();

        Thread1 thread1 = new Thread1(example);
        thread1.start();

        Example example2 = new Example();

        Thread2 thread2 = new Thread2(example2);
        thread2.start();


//       for (int i=0; i<1;i++){
//           Example example = new Example();
//           new Thread(new Runnable() {
//               @Override
//               public void run() {
//                   example.execute();
//               }
//           }).start();
//
//           example = new Example();
//           new Thread(new Runnable() {
//               @Override
//               public void run() {
//                   new Example().execute2();
//               }
//           }).start();
//       }
    }
}

class  Example{
    public synchronized static void execute(){
        for (int i=0; i<10; i++){
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("Hello," + i);
        }
    }
    public synchronized static void execute2(){
        for (int i=0; i<10; i++){
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("world," + i);
        }
    }
}

class Thread1 extends  Thread{

    private Example example;

    public Thread1(Example example){
        this.example = example;
    }

    @Override
    public void run()
    {
        example.execute();
    }

}

class Thread2 extends  Thread{

    private Example example;

    public Thread2(Example example){
        this.example = example;
    }

    @Override
    public void run()
    {
        example.execute2();
    }

}