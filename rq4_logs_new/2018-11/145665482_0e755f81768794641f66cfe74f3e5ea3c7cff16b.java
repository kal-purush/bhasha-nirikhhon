package cuncorrent.lock;

/**
 * 产生死锁的一个简单例子
 * @author wangjianlou 2018年11月12日
 * @version V1.0
 */
public class DeadLock {
	
	private static final String LOCK_A = "A";
	private static final String LOCK_B = "B";
	
	public static void main(String[] args) {
		Thread a = new Thread(new Runnable() {
			
			@Override
			public void run() {
				synchronized (DeadLock.LOCK_A) {
					System.out.println("a获得 lock_a");
					try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					synchronized (DeadLock.LOCK_B) {
						System.out.println("a获得 lock_b");
					}
				}
				
			}
		});
		
		Thread b = new Thread(new Runnable() {
			
			@Override
			public void run() {
				synchronized (DeadLock.LOCK_B) {
					System.out.println("b获得 lock_b");
					try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					synchronized (DeadLock.LOCK_A) {
						System.out.println("b获得 lock_a");
					}
				}
				
			}
		});
		a.start();
		b.start();
	}
}