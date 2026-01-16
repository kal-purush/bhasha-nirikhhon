package codes;

import java.time.LocalDateTime;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class CachedThreadAdmin {
	private final ThreadPoolExecutor threadAdmin = (ThreadPoolExecutor) Executors.newCachedThreadPool();
	
	public void execute(Pow pow) {
		try {
			threadAdmin.execute(pow);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void shutdown() throws InterruptedException {
		System.out.println(String.format(Constants.ADMIN_SHUT_DOWN, LocalDateTime.now().format(Constants.TIME_FORMATTER)));
        threadAdmin.shutdown();
        System.out.println(String.format(Constants.SHOW_MAX_THREAD_NUMBER, threadAdmin.getLargestPoolSize()));
        if (!threadAdmin.awaitTermination(500, TimeUnit.MILLISECONDS)) {
            System.out.println(String.format(Constants.SHUTDOWN_TIMEOUT, LocalDateTime.now().format(Constants.TIME_FORMATTER)));
        }
    }
	
	public void shutdownNow() throws InterruptedException {
		System.out.println(String.format(Constants.ADMIN_SHUT_DOWN, LocalDateTime.now().format(Constants.TIME_FORMATTER)));
        threadAdmin.shutdownNow();
        System.out.println(String.format(Constants.SHOW_MAX_THREAD_NUMBER, threadAdmin.getLargestPoolSize()));
        if (!threadAdmin.awaitTermination(500, TimeUnit.MILLISECONDS)) {
            System.out.println(String.format(Constants.SHUTDOWN_TIMEOUT, LocalDateTime.now().format(Constants.TIME_FORMATTER)));
        }
    }


}