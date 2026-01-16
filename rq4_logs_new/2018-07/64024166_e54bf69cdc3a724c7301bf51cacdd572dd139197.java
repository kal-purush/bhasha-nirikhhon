package com.ubs.sai;

public interface SimpleQueue<T> {

	void offer(T string);

	T poll();

	int getRear();

	int getFront();

	int getSize();

}