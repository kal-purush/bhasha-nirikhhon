package test.queue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedTransferQueue;

class LTQProducer<T> implements Runnable
{
	LinkedTransferQueue<T> queue;
	List<T> elements;
	public LTQProducer(LinkedTransferQueue<T> queue, List<T> elements) 
	{
		super();
		this.queue = queue;
		this.elements = elements;
	}

	@Override
	 public void run() {
	      System.out.println("Producer Started");
	      try {	    	 
	         while(!elements.isEmpty())
	         {	
	        	 Thread.sleep(1000);
	        	 T elem = elements.remove(0);	        	 
	        	 queue.transfer(elem);
	        	 System.out.println("Producer put the element = "+elem);
	         }
	      } catch (Exception ex) {
	         ex.printStackTrace();
	      }
	 }
}
public class ProducerConsumerUsingLTQ 
{
	public static void main(String[] args) {
		LinkedTransferQueue<String> bq = new LinkedTransferQueue<>();
		List<String> elements = new ArrayList<>();
		for(int i=0;i<10;i++)
			elements.add("elem"+i);
		Thread producer = new Thread(new LTQProducer(bq,elements));
	    Thread consumer = new Thread(new Consumer(bq,elements, "Consumer"));
	    producer.start();
	    consumer.start();
	}
}