import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class FairProducerConsumers {
	public static void main(String[] args) {
		BlockingQueue<String> bq = new ArrayBlockingQueue<>(10,false);
		List<String> elements = new ArrayList<>();
		for(int i=0;i<10;i++)
			elements.add("elem"+i);
		Thread producer = new Thread(new Producer(bq,elements));
	    Thread consumer1 = new Thread(new Consumer(bq,elements,"Consumer1"));
	    Thread consumer2 = new Thread(new Consumer(bq,elements,"Consumer2"));
	    producer.start();
	    consumer1.start();
	    consumer2.start();
	}
}