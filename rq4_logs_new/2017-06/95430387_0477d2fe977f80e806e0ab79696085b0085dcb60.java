package interruption;

public class InterruptWaitingThread {
	public static void main(String[] args) throws InterruptedException {
		Object lock = new Object();
		Runnable r = ()->{
			synchronized(lock)
			{
				int counter = 1;
				while(counter<100)
					System.out.println(counter+" square = "+Math.pow(counter++, 2));
			}
		};
		Thread t = new Thread(r);
		synchronized (lock) {
			t.start();
			Thread.sleep(5*1000);
			t.interrupt();
			Thread.sleep(5*1000);
			System.out.println(t.isInterrupted()+","+t.isAlive());
		}
	}
}