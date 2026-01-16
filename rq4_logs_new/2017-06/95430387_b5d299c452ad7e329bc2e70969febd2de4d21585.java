import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class ReentrantLockTryLockTest 
{
	public static void main(String[] args) throws InterruptedException 
	{
		ReentrantLock lock = new ReentrantLock();
		Runnable r = ()->
		{
			try 
			{
				if(lock.tryLock(4, TimeUnit.SECONDS))
				{
					try
					{
						
						lock.lock();
						System.out.println(Thread.currentThread().getName()+" got the lock");
						Thread.sleep(10*1000);
						System.out.println(Thread.currentThread().getName()+" executed");
					} catch (InterruptedException e) 
					{
						e.printStackTrace();
						System.out.println(Thread.currentThread().isInterrupted());
						Thread.interrupted();
					}
					finally 
					{
						lock.unlock();
					}
				}
				else
				{
					System.out.println(Thread.currentThread().getName()+" couldnt get the lock");
				}
			} 
			catch (Exception e) 
			{
				
				e.printStackTrace();
			}
			
		};
		
		Thread t1 = new Thread(r, "T1");
		Thread.sleep(1*1000);
		Thread t2 = new Thread(r, "T2");
		t1.start();
		t2.start();	
		Thread.sleep(2*1000);
		t2.interrupt();
	}
}