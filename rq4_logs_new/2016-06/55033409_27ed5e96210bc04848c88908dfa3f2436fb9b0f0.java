import java.util.ArrayList;
import java.util.Random;

/**
 * Created by shinsy1023 on 2016. 5. 9..
 */
public class RandomNum {
    public static void main(String[] args){
        long rand;
        Random rng=new Random();
        long i;
        ArrayList<Long> randList=new ArrayList<Long>();
        for(i=0; i<100000; i++){
            rand=rng.nextInt(200000);
            while(randList.contains(rand)){
                rand=rng.nextInt(200000);
            }
            randList.add(rand);
        }
        System.out.println(randList);
    }
}