package cn.edu.nju.cs.itrace4.demo.getPaperData;

import java.util.ArrayList;
import java.util.List;

public class GetAPAndMAPAllPercent {
	
	public void bactchProcess(String basePath) throws InterruptedException {
		List<Thread> threads = new ArrayList<Thread>();
		for(double percent=0.1; percent<=1.0;percent+=0.1) {
			GetApAndMap task = new GetApAndMap(percent,basePath);
			Thread thread = new Thread(task);
			threads.add(thread);
			thread.start();
		}
		
		for(Thread thread:threads) {
			thread.join();
		}
		System.out.println("----end-----");
	}
	
	public static void main(String[] args) throws InterruptedException {
		GetAPAndMAPAllPercent batchTool = new GetAPAndMAPAllPercent();
		batchTool.bactchProcess("./paperData");
	}
}