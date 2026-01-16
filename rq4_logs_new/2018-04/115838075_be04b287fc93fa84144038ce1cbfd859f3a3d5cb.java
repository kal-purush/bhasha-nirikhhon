package ThreadDemo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author qinc
 * @version V1.0
 * @Description: Condition 实现类似生产消费
 * @Date 2018/2/23 16:51
 */
public class ConditionTest {

    public static void main(String args[]){

        ReentrantLock lock= new ReentrantLock();
        Condition notFull=lock.newCondition();
        Condition notEmp =lock.newCondition();

        List<String> pool=new ArrayList<>();
        //生产
        new Thread(new Runnable() {
            @Override
            public void run() {
                lock.lock();
                try {
                    while (true){
                        if(pool.size()<10){
                            System.out.println("生产了一个产品");
                            pool.add("产品");
                            notEmp.signal();
                        }else{
                            try {
                                System.out.println("队列满了，不在生产了");
                                notFull.await();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }finally {
                    lock.unlock();
                }

            }
        }).start();

        //消费
        new Thread(new Runnable() {
            @Override
            public void run() {
                lock.lock();
                try {
                    while (true){
                        if (pool.size()==10){
                            System.out.println("一次全部消费完了");
                            pool.clear();
                            System.out.println("通知工厂继续生产！");
                            notFull.signal();

                        }else{
                            notEmp.await();
                        }
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }finally {
                    lock.unlock();
                }
            }
        }).start();

    }

}
