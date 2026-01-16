package threads.kuangStudy.thread;

/**
 * Join 合并线程，待此线程执行完成后，在执行其他线程，其他线程阻塞
 * 可以想象成插队
 * @author Zenghui Wang
 * @create 2021-06-23 1:41 PM
 */
public class ThreadJoin101 implements Runnable{

    @Override
    public void run() {
        System.out.println("线程 插队");
    }

    public static void main(String[] args) throws InterruptedException {
        ThreadJoin101 threadJoin101 = new ThreadJoin101();
        Thread thread = new Thread(threadJoin101);
        thread.start();
        for (int i = 0; i < 200; i++) {
            if(i==100){
                thread.join();
            }
            System.out.println(i);
        }
    }
}

// 测试join
class myJoin implements Runnable{
    @Override
    public void run() {
        System.out.println("");
    }
}