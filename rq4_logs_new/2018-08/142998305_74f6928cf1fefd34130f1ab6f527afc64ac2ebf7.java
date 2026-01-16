package com.ymagis.daf.game;

import com.ymagis.daf.response.StartResponse;
import com.ymagis.daf.response.TestResponse;

public abstract class Game {

	private int callCount;

	public int getCallCount() {
		return callCount;
	}

	public Game() {
		callCount = 0;
	}

	public abstract StartResponse start();

	public TestResponse test(String proposition) {
		callCount++;
		return null;
	};
}