package bench;

import java.util.Random;

public class PopulateArray implements IBenchmark {

    private int threadnr;
    @Override
    public void initialize(Object... params) {
        threadnr=(Integer)params[0];
    }

    @Override
    public void warmUp() {
        int size=1000;
        int arraytest[][]=new int[size][size];
        int upperbound=1000;
        Random random=new Random();
        for(int i=0;i<size;i++){
            for(int j=0;j<size;j++)
                arraytest[i][j]=random.nextInt(upperbound);
        }
    }

    @Override
    public void run() {
        int size=threadnr*1000;
        int array[][]=new int[size][size];
        Thread t;
        for(int i=0;i<threadnr;i++){
            t=new Thread(new PopulateArrayRunnable(i,array,size,Integer.toString(i)));
            t.start();
            try{
                t.join();
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }
    }

    @Override
    public void run(Object... params) {

    }

    @Override
    public void cancel() {

    }

    @Override
    public void clean() {

    }
}