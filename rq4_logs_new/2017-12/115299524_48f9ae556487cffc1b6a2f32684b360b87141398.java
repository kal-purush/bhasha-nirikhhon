package com.fuiou.designpatterns.bridge;

public abstract class DBbridge {

	private Driver driver;

	public Driver getDriver() {
		return driver;
	}

	public void setDriver(Driver driver) {
		this.driver = driver;
	}
	
	public void driver(){
		driver.driver();
	}
}