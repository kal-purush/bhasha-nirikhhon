package com.coding.challenge.linked.list;

import com.coding.challenge.ListNode;

class RotateList {
	RotateList() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final ListNode<Integer> l1 = new ListNode<Integer>(1,
				new ListNode<>(2, new ListNode<>(3, new ListNode<>(4, new ListNode<>(5)))));
		assert "45123".equals(rotateRight(l1, 2).toString());

		final ListNode<Integer> l2 = new ListNode<>(0, new ListNode<>(1, new ListNode<>(2)));
		assert "201".equals(rotateRight(l2, 4).toString());

		assert rotateRight(null, 1) == null;
	}

	static ListNode<Integer> rotateRight(ListNode<Integer> head, int k) {
		if (head == null)
			return null;
		int n = 0;
		ListNode<Integer> tail = null;
		ListNode<Integer> curr = head;
		while (curr != null) {
			n = n + 1;
			tail = curr;
			curr = curr.next;
		}

		final int offset = n - k % n;
		if (offset == n)
			return head;
		ListNode<Integer> leftEnd = head;
		for (int i = 1; i < offset; i++)
			leftEnd = leftEnd.next;

		tail.next = head;
		final ListNode<Integer> h = leftEnd.next;
		leftEnd.next = null;
		return h;
	}
}