package com.xjdl.juc.cas;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

@Slf4j
class SpinLockDemoTest {
	@Test
	void test() throws InterruptedException {
		SpinLockDemo spinLockDemo = new SpinLockDemo();
		Thread t1 = new Thread(() -> {
			//加锁
			spinLockDemo.lock();
			try {
				//A 线程业务
				log.info("Thread A 开始执行业务！");
				TimeUnit.SECONDS.sleep(5);
				log.info("Thread A 执行完成！");
			} catch (InterruptedException e) {
				log.error(e.getMessage(), e);
			}
			spinLockDemo.unlock();
		}, "A线程");

		try {
			TimeUnit.MILLISECONDS.sleep(500);
		} catch (InterruptedException e) {
			log.error(e.getMessage(), e);
		}

		Thread t2 = new Thread(() -> {
			spinLockDemo.lock();
			log.info("B 线程开始执行业务！");
			try {
				TimeUnit.SECONDS.sleep(2);
			} catch (InterruptedException e) {
				log.error(e.getMessage(), e);
			}
			log.info("B 线程执行完成！");
			spinLockDemo.unlock();
		}, "B线程");

		t1.start();
		t2.start();
		t1.join();
		t2.join();
	}
}