package pl.edu.agh.kis;

/**
 * Simple program-configuration class
 * @author szymon
 *
 */
public class Config {
	/**
	 * Queue capacity
	 */
	final int QUEUE_CAPACITY;
	/**
	 * Cache capacity
	 */
	final int CACHE_CAPACITY;
	
	/**
	 * Default constructor
	 * @param queueCapacity
	 * @param cacheCapacity
	 */
	Config(int queueCapacity, int cacheCapacity) {
		QUEUE_CAPACITY = queueCapacity;
		CACHE_CAPACITY = cacheCapacity;
	}

	/**
	 * @return
	 * QUEUE_CAPACITY
	 */
	public int getQueueCapacity() {
		return QUEUE_CAPACITY;
	}

	/**
	 * @return
	 * CACHE_CAPACITY
	 */
	public int getCacheCapacity() {
		return CACHE_CAPACITY;
	}
}