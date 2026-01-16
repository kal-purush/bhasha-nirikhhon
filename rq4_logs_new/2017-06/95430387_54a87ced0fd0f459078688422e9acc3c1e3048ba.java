import java.util.List;
import java.util.concurrent.BlockingQueue;

public class Consumer<T> implements Runnable {

	BlockingQueue<T> queue;
	List<T> elements;
	String name;
	public Consumer(BlockingQueue<T> queue, List<T> elements, String name) 
	{
		super();
		this.queue = queue;
		this.elements = elements;
		this.name = name;
	}
	@Override
	 public void run() {		      
	      System.out.println(name+" Started");
	      while(true)
	      {		    	  
	    	  try {	    		
				System.out.println(name+" consumed = "+queue.take());
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
	      }
	 }

}