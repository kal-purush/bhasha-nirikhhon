import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class ProducerConsumerUsingBQ {
	
	public static void main(String[] args) {
		BlockingQueue<String> bq = new ArrayBlockingQueue<>(10);
		List<String> elements = new ArrayList<>();
		for(int i=0;i<10;i++)
			elements.add("elem"+i);
		Thread producer = new Thread(new Producer(bq,elements));
	    Thread consumer = new Thread(new Consumer(bq,elements, "Consumer"));
	    producer.start();
	    consumer.start();
	}
}