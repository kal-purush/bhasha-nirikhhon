import java.util.List;
import java.util.concurrent.BlockingQueue;

class Producer<T> implements Runnable {
	BlockingQueue<T> queue;
	List<T> elements;
	public Producer(BlockingQueue<T> queue, List<T> elements) 
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
	        	 queue.put(elements.remove(0));
	         }
	      } catch (Exception ex) {
	         ex.printStackTrace();
	      }
	 }
	
}