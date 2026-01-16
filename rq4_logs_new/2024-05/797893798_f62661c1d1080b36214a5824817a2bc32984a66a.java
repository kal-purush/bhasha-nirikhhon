package com.multithreading.extendingRunnable;

public class MultiThreadingDemo implements Runnable {

	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			System.out.println("Thread :"+Thread.currentThread().getId());			
		}catch(Exception e) {
			System.out.println("exception caught");
		}
	}

}