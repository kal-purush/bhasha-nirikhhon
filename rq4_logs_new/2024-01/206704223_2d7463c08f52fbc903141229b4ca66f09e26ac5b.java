package com.coding.challenge.queue;

import java.util.ArrayDeque;
import java.util.Deque;

class MyCircularDeque {
	private final Deque<Integer> deque;
	private final int k;

	MyCircularDeque(int k) {
		this.k = k;
		deque = new ArrayDeque<>(k);
	}

	public static void main(String[] args) {
		final MyCircularDeque myCircularDeque = new MyCircularDeque(3);
		assert myCircularDeque.insertLast(1); // return True
		assert myCircularDeque.insertLast(2); // return True
		assert myCircularDeque.insertFront(3); // return True
		assert !myCircularDeque.insertFront(4); // return False, the queue is full.
		assert myCircularDeque.getRear() == 2; // return 2
		assert myCircularDeque.isFull(); // return True
		assert myCircularDeque.deleteLast(); // return True
		assert myCircularDeque.insertFront(4); // return True
		assert myCircularDeque.getFront() == 4; // return 4
	}

	public boolean insertFront(int value) {
		if (deque.size() == k)
			return false;
		return deque.offerFirst(value);
	}

	public boolean insertLast(int value) {
		if (deque.size() == k)
			return false;
		return deque.offer(value);
	}

	public boolean deleteFront() {
		if (deque.isEmpty())
			return false;
		deque.poll();
		return true;
	}

	public boolean deleteLast() {
		if (deque.isEmpty())
			return false;
		deque.pollLast();
		return true;
	}

	public int getFront() {
		if (deque.isEmpty())
			return -1;
		return deque.peek();
	}

	public int getRear() {
		if (deque.isEmpty())
			return -1;
		return deque.peekLast();
	}

	public boolean isEmpty() {
		return deque.isEmpty();
	}

	public boolean isFull() {
		return deque.size() == k;
	}
}