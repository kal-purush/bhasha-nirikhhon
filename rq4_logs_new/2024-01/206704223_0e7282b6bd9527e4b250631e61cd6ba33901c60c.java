package com.coding.challenge.linked.list;

import com.coding.challenge.ListNode;

class OddEvenLinkedList {
	OddEvenLinkedList() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final ListNode<Integer> h1 = new ListNode<>(1,
				new ListNode<>(2, new ListNode<>(3, new ListNode<>(4, new ListNode<>(5)))));
		assert "13524".equals(oddEvenList(h1).toString());

		final ListNode<Integer> h2 = new ListNode<Integer>(2, new ListNode<>(1,
				new ListNode<>(3, new ListNode<>(5, new ListNode<>(6, new ListNode<>(4, new ListNode<>(7)))))));
		assert "2367154".equals(oddEvenList(h2).toString());

		assert oddEvenList(null) == null;

		assert "1".equals(oddEvenList(new ListNode<>(1)).toString());

		assert "12".equals(oddEvenList(new ListNode<>(1, new ListNode<>(2))).toString());
	}

	static ListNode<Integer> oddEvenList(ListNode<Integer> head) {
		if (head == null)
			return null;
		ListNode<Integer> firstEven = head.next;
		ListNode<Integer> lastOdd = null;
		int c = 1;
		ListNode<Integer> curr = head;
		while (curr.next != null) {
			if (c % 2 == 1)
				lastOdd = curr;
			final ListNode<Integer> tmp = curr.next;
			curr.next = tmp.next;
			curr = tmp;
			c = c + 1;
		}
		if (c % 2 == 1)
			lastOdd = curr;
		lastOdd.next = firstEven;
		return head;
	}
}