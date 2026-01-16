package com.coding.challenge.general;

import java.util.Arrays;
import java.util.Iterator;

class PeekingIterator implements Iterator<Integer> {
	private final Iterator<Integer> it;
	private int head = 0;

	PeekingIterator(Iterator<Integer> iterator) {
		// initialize any member here.
		it = iterator;
	}

	public static void main(String[] args) {
		final PeekingIterator peekingIterator = new PeekingIterator(Arrays.asList(1, 2, 3).iterator());
		assert peekingIterator.next() == 1;
		assert peekingIterator.peek() == 2;
		assert peekingIterator.next() == 2;
		assert peekingIterator.next() == 3;
		assert !peekingIterator.hasNext();
	}

	// Returns the next element in the iteration without advancing the iterator.
	public Integer peek() {
		// is this the first peek for this element.
		if (head == 0)
			head = it.next();
		return head;
	}

	// hasNext() and next() should behave the same as in the Iterator interface.
	// Override them if needed.
	@Override
	public Integer next() {
		if (head > 0) {
			final int tmp = head;
			head = 0;
			return tmp;
		}
		return it.next();
	}

	@Override
	public boolean hasNext() {
		if (head > 0)
			return true;
		return it.hasNext();
	}
}