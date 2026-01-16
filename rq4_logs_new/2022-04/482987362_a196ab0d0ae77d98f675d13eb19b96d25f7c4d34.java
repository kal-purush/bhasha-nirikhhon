package src.ru.avdotchenkov.multithreadingLesson;

public class MultithreadingLesson4_1 {
	static class Print {
		String letter = "A";
		int count = 0;
		
		public int getCount () {
			return count;
		}
		
		public synchronized void printA () {
			while (letter != "A") {
				try {
					wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
				System.out.print(Thread.currentThread().getName());
				count++;
				letter = "B";
				notifyAll();
				
			}
		}
		
		public synchronized void printB () {
			while (letter != "B") {
				try {
					wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
				System.out.print(Thread.currentThread().getName());
				count++;
				letter = "C";
				notifyAll();
				
			}
		}
		
		public synchronized void printC () {
			while (letter != "C") {
				try {
					wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			System.out.print(Thread.currentThread().getName());
			count++;
			letter = "A";
			notifyAll();
			
		}
	}
	
	static class MyThread implements Runnable {
		private Print print;
		
		public MyThread (Print print) {
			this.print = print;
		}
		
		@Override public void run () {
			
			while (print.getCount() < 16) {
				if (Thread.currentThread().getName().equals("A")) {
					print.printA();
				} else if (Thread.currentThread().getName().equals("B")) {
					print.printB();
				} else if (Thread.currentThread().getName().equals("C")) {
					print.printC();
				}
			}
			return;
		}
	}
}

