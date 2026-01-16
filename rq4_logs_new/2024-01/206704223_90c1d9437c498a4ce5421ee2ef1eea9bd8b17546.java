package com.coding.challenge.stack;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;

import com.coding.challenge.ListNode;

class NextGreaterNodeInLinkedList {
	NextGreaterNodeInLinkedList() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final ListNode<Integer> l1 = new ListNode<>(2, new ListNode<>(1, new ListNode<>(5)));
		final int[] a1 = { 5, 5, 0 };
		assert Arrays.equals(a1, nextLargerNodes(l1));

		final ListNode<Integer> l2 = new ListNode<>(2,
				new ListNode<>(7, new ListNode<>(4, new ListNode<>(3, new ListNode<>(5)))));
		final int[] a2 = { 7, 0, 5, 5, 0 };
		assert Arrays.equals(a2, nextLargerNodes(l2));
	}

	static int[] nextLargerNodes(ListNode<Integer> head) {
		int n = 0;
		for (ListNode<Integer> curr = head; curr != null; curr = curr.next)
			n = n + 1;

		final int[] a = new int[n];
		final int[] nums = new int[n];
		// monotonically decreasing stack.
		final Deque<Integer> s = new ArrayDeque<>();
		int i = 0;
		for (ListNode<Integer> curr = head; curr != null; curr = curr.next, i = i + 1) {
			nums[i] = curr.val;
			while (!s.isEmpty() && nums[s.peek()] < curr.val) {
				a[s.peek()] = curr.val;
				s.pop();
			}
			s.push(i);
		}
		return a;
	}
}