package Thread;

import java.util.ArrayList;
import java.util.List;

/**
 * @description:原子性问题
 * @auth tongchenghao
 * @date 2020/2/2
 */
public class AtomicDemo {
    private volatile static int count = 0;

    public static void inc(){
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        count = count+1;
    }

    public static void main(String[] args) throws InterruptedException {
        /**
         * 按理说有1000个线程则count应该为1000，而运行结果往往小于1000
         */
        for (int i=0;i<1000;i++){
            Thread thread = new Thread(AtomicDemo::inc);
            thread.start();
        }
        Thread.sleep(4000);
        System.out.println(count);
    }
}