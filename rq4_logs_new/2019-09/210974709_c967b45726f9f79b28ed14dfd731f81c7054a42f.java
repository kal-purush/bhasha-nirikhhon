package com.csk.collection.reptile;

import com.csk.collection.reptile.cache.IReptileCache; 

public class ApplicationContextConverter {
	/**
	 * 缓存对象
	 */
	private IReptileCache cache; 
	public IReptileCache getCache() {
		return cache;
	}
	public void setCache(IReptileCache cache) {
		this.cache = cache;
	}
	
}