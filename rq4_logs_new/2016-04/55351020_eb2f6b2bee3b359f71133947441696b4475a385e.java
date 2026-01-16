package com.quirkygaming.propertydb;


public interface CustomScheduler {
	public void scheduleRepeatingTask(InitializationToken token, int period_millis, Runnable r);
	public void onDatabaseClose();
}