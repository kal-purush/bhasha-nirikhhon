import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

public class ProducerConsumerUsingPriorityBQ 
{	
	
	public static void main(String[] args) throws InterruptedException {
		ProducerConsumerUsingBQ testBQ = new ProducerConsumerUsingBQ();
		Comparator<Integer> comparator = (o1,o2) -> {
			return o2.compareTo(o1);
		};
		BlockingQueue<Integer> bq = new PriorityBlockingQueue(10,comparator);
		List<Integer> elements = new ArrayList<>();
		for(int i=0;i<10;i++)
			elements.add(i);
		Thread producer = new Thread(new Producer(bq,elements));
	    Thread consumer = new Thread(new Consumer(bq,elements,"Consumer"));
	    producer.start();
	    consumer.start();
	    producer.join();	    
	}
}