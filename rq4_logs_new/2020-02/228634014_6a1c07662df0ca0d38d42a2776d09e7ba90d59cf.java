package callable;

import java.util.concurrent.*;

/**
 * @description:callable有返回值的多线程
 * @auth tongchenghao
 * @date 2020/2/1
 */
public class CallableDome implements Callable<String> {

    public static void main(String[] args) {
        ExecutorService executorService = Executors.newCachedThreadPool();
        CallableDome callableDome = new CallableDome();
        Future<String> future = executorService.submit(callableDome);
        try {
            //get时线程阻塞，调用 CallableDome 的 call() 方法
            String callBack = future.get();
            System.out.println(callBack);
            executorService.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String call() throws Exception {
        return "String" + 1;
    }
}