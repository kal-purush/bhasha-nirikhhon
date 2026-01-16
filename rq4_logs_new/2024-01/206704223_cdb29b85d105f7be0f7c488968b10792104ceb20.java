package com.coding.challenge.linked.list;

import com.coding.challenge.ListNode;

class SwapNodesInPairs {
	SwapNodesInPairs() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final ListNode<Integer> l1 = new ListNode<>(1, new ListNode<>(2, new ListNode<>(3, new ListNode<>(4))));
		assert "2143".equals(swapPairs(l1).toString());

		assert "1".equals(swapPairs(new ListNode<>(1)).toString());

		final ListNode<Integer> l2 = new ListNode<>(1,
				new ListNode<>(2, new ListNode<>(3, new ListNode<>(4, new ListNode<>(5, new ListNode<>(6))))));
		assert "214365".equals(swapPairs(l2).toString());

		final ListNode<Integer> l3 = new ListNode<>(1,
				new ListNode<>(2, new ListNode<>(3, new ListNode<>(4, new ListNode<>(5)))));
		assert "21435".equals(swapPairs(l3).toString());
	}

	static ListNode<Integer> swapPairs(ListNode<Integer> head) {
		if (head == null || head.next == null)
			return head;
		final ListNode<Integer> start = head.next;
		ListNode<Integer> curr = head;
		ListNode<Integer> prev = new ListNode<>(-1);
		while (curr != null && curr.next != null) {
			final ListNode<Integer> next = curr.next;
			ListNode<Integer> afterNext = null;
			if (next != null)
				afterNext = next.next;
			next.next = curr;
			curr.next = afterNext;
			prev.next = next;
			prev = curr;
			curr = curr.next;
		}
		return start;
	}
}