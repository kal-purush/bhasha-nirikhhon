package info.yukay.projectz;

public class Projectile {

	Thread ThreadA;
	int TrSpeed1 = 10;
	
	public void init() {
		Thread ThreadA = new Thread (Threadrun);
		ThreadA.start();	
	}
	
	
	Runnable Threadrun = new Runnable(){


	public void run() {
		while (true) {
			System.out.println("Runned");
			try {
			ThreadA.sleep(TrSpeed1);	
			}
			catch (InterruptedException ex){}
			}
		}
	};
}

