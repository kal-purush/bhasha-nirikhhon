package concurrency.arrayblockingqueues;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;

import static concurrency.producerconsumer.ThreadColor.*;

public class AnotherMain {
	public static final String EOF = "EOF";

	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		List<String> buffer = new ArrayList<>();
//		ReentrantLock bufferLock = new ReentrantLock();
//		It's a queue, works as FIFO, so that's we use put,take,peek
		ArrayBlockingQueue<String> buffer = new ArrayBlockingQueue<>(6);

		ExecutorService executorService = Executors.newFixedThreadPool(5);

		MyProducer producer = new MyProducer(buffer, ANSI_CYAN);
		MyConsumer consumer1 = new MyConsumer(buffer, ANSI_PURPLE);
		MyConsumer consumer2 = new MyConsumer(buffer, ANSI_GREEN);

//		new Thread(producer).start();
//		new Thread(consumer1).start();
//		new Thread(consumer2).start();
		executorService.execute(producer);
		executorService.execute(consumer1);
		executorService.execute(consumer2);

		Future<String> future = executorService.submit(new Callable<String>() {

			@Override
			public String call() throws Exception {
				// TODO Auto-generated method stub
				System.out.println(ANSI_RED + " I;m being printed from the callable class");
				return "This is a callable result";
			}
		});

		try {
			System.out.println(future.get());
		} catch (ExecutionException ex) {
			System.out.println("Something went wrong");
		} catch (InterruptedException ex) {
			System.out.println("Thread running the task was interrupted");
		}

		executorService.shutdown();
	}

}

class MyProducer implements Runnable {
	private ArrayBlockingQueue<String> buffer;
	private String color;

	public MyProducer(ArrayBlockingQueue<String> buffer, String color) {
		this.buffer = buffer;
		this.color = color;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		Random random = new Random();
		String[] nums = { "1", "2", "3", "4", "5" };

		for (String num : nums) {
			try {
				System.out.println(color + "Adding..." + num);
				// add and remove can throw exceptions
				// put add a block, also is a thread-safe method
				buffer.put(num);

//				bufferLock.lock();
//				try {
//					buffer.add(num);
//				} finally {
//					bufferLock.unlock();
//				}

				Thread.sleep(random.nextInt(2000));
			} catch (InterruptedException e) {
				System.out.println("Producer was interrupted");
			}
		}
		try {
			buffer.put("EOF");
		} catch (InterruptedException e) {
		}

//		bufferLock.lock();
//		try {
//			buffer.add("EOF");
//		} finally {
//			bufferLock.unlock();
//		}

	}

}

class MyConsumer implements Runnable {

	private ArrayBlockingQueue<String> buffer;
	private String color;

	public MyConsumer(ArrayBlockingQueue<String> buffer, String color) {
		this.buffer = buffer;
		this.color = color;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		int counter = 0;
		while (true) {
			/*
			 * Error because hold the lock to much time and have too many lock the if
			 * statement with the empty makes the loop continues without unlocking Example
			 * explanation down
			 * 
			 * 
			 * bufferLock.lock(); if (buffer.isEmpty()) { continue; } if
			 * (buffer.get(0).equals(AnotherMain.EOF)) { System.out.println(color +
			 * "Exiting"); break; } else { System.out.println(color + "Removed " +
			 * buffer.remove(0)); } bufferLock.unlock();
			 */

			/**
			 * One approach
			 * 
			 * bufferLock.lock(); if (buffer.isEmpty()) { bufferLock.unlock(); continue; }
			 * if (buffer.get(0).equals(AnotherMain.EOF)) { System.out.println(color +
			 * "Exiting"); bufferLock.unlock(); break; } else { System.out.println(color +
			 * "Removed " + buffer.remove(0)); } bufferLock.unlock();
			 * 
			 */

//			Correct aproach
			// bufferLock.lock();
//			if (bufferLock.tryLock()) {
//				try {
//					if (buffer.isEmpty()) {
//						continue;
//					}
//					System.out.println(color+"The counter = " + counter);
//					counter =0;
//					if (buffer.get(0).equals(AnotherMain.EOF)) {
//						System.out.println(color + "Exiting");
//						break;
//					} else {
//						System.out.println(color + "Removed " + buffer.remove(0));
//					}
//				} finally {
//					bufferLock.unlock();
//				}
//			}else {
//				counter++;
//			}
			synchronized (buffer) {
				try {
					if (buffer.isEmpty()) {
						continue;
					}
					if (buffer.peek().equals(AnotherMain.EOF)) {
						System.out.println(color + "Exiting");
						break;
					} else {
						System.out.println(color + "Removed " + buffer.take());
					}
				} catch (InterruptedException e) {
				}
			}

		}
	}

//	ReentrantLock lock1;
//	public void methodA() {
//		lock1.lock();
//		methodB();
//		[more code]
//		lock1.unlock();		
//	}
//	
//	public void methodB() {
//		[some code]
//		lock1.lock();
//		[more code]
//		lock1.unlock();
//		[more code]
//	}

}