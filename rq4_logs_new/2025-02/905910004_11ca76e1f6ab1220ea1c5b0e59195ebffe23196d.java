package lesson_13_Extended_Synchronization.executor_service;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Main {
    public static void main(String[] args) {
        Main main = new Main();
        try (ExecutorService executorService = main.generationExecutor(10)) {
            for (int i = 0; i < 150; i++) {
              Future<?> future =  executorService.submit(() -> System.out.println("test"));
              future.get();
            }
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public ExecutorService generationExecutor(int threads) {
        return Executors.newFixedThreadPool(threads);
    }

}