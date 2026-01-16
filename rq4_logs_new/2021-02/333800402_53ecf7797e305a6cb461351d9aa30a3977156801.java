package JAVA_LEVEL_2.Java_lvl2_homework.git.lesson5.lection5;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class ExecutorServiceTest1 {
    public static void main(String ... args) {
        ExecutorService e1 = Executors.newFixedThreadPool(15);
        /*for(int i = 0; i<100; i++) {
            e1.submit(() -> System.out.println(Thread.currentThread()));
        }
        e1.shutdown();*/
        //List<Future<String>> futures = new ArrayList<>();
        List<Future<Long>> futures = new ArrayList<>();
        Callable c =() -> Thread.currentThread().toString();
        for(int i = 0; i<=10; i++) {
            /*try {
                e1.submit(c).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }*/
            futures.add(e1.submit(new CallableImplementation((long)i)));
        }
        e1.shutdown();
        Long finalResult = 0l;
        for(Future<Long> futureLong: futures) {
            try {
                finalResult+=futureLong.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        System.out.println(finalResult);
    }

    private void exemple() {

        synchronized(this) {
            //thread safe...
        }
    }
}

class CallableImplementation implements Callable<Long> {
    private Long startNumber;

    public CallableImplementation(Long startNumber){
        this.startNumber = startNumber;
    }

    @Override
    public Long call() throws Exception {
        if(startNumber<=1) {
            return 1l;
        }
        Long result = 1l;
        for(int i=1; i<=startNumber; i++){
            result*=i;
        }
        Thread.sleep(100);
        return result;
    }
}