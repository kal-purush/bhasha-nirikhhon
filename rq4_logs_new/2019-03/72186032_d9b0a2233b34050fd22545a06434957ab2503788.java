package com.thread;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LeakThreads {


    public static void main(String[] args) {

        System.out.println("available processors " + Runtime.getRuntime().availableProcessors());
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        PrintDataSpooler printDataSpooler = new PrintDataSpooler();
        scheduledExecutorService.scheduleAtFixedRate(printDataSpooler, 0, 5, TimeUnit.SECONDS);
    }
}

class PrintDataSpooler implements Runnable {

    @Override
    public void run() {
        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        for (int i = 0; i < 10; i++) {
            executorService.submit(new PrintData(i, "printing data with id " + i));
        }

        //thread leak -- executor not shutdown.
    }
}

class PrintData implements Runnable {

    private final int id;
    private final String data;

    PrintData(int id, String data) {
        this.id = id;
        this.data = data;
    }

    @Override
    public void run() {
        LocalDateTime now = LocalDateTime.now();
        System.out.println(String.format("[%s] Print Data %d started", now, id));
        System.out.println(String.format("[%s] PrintData[%d] --> %s", now, id, data));
        System.out.println(String.format("[%s] Print Data %d completed", now, id));
    }
}