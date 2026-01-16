package com.coding.challenge.linked.list;

import com.coding.challenge.ListNode;

class RemoveDuplicatesFromSortedListII {
	RemoveDuplicatesFromSortedListII() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final ListNode<Integer> head1 = new ListNode<>(1, new ListNode<>(2,
				new ListNode<>(3, new ListNode<>(3, new ListNode<>(4, new ListNode<>(4, new ListNode<>(5)))))));
		assert "125".equals(deleteDuplicates(head1).toString());

		final ListNode<Integer> head2 = new ListNode<>(1,
				new ListNode<>(1, new ListNode<>(1, new ListNode<>(2, new ListNode<>(3)))));
		assert "23".equals(deleteDuplicates(head2).toString());

		final ListNode<Integer> head3 = new ListNode<Integer>(1,
				new ListNode<>(1, new ListNode<>(2, new ListNode<>(3, new ListNode<>(3)))));
		assert "2".equals(deleteDuplicates(head3).toString());

		final ListNode<Integer> head4 = new ListNode<Integer>(1,
				new ListNode<>(2, new ListNode<>(3, new ListNode<>(3, new ListNode<>(3)))));
		assert "12".equals(deleteDuplicates(head4).toString());
	}

	static ListNode<Integer> deleteDuplicates(ListNode<Integer> head) {
		final ListNode<Integer> dummy = new ListNode<Integer>(-101, head);
		ListNode<Integer> prev = dummy;
		ListNode<Integer> curr = head;
		while (curr != null && curr.next != null) {
			if (curr.val == curr.next.val) {
				final int dupKey = curr.val;
				while (curr != null && curr.val == dupKey)
					curr = curr.next;
				prev.next = curr;
			} else {
				prev = curr;
				curr = curr.next;
			}
		}
		return dummy.next;
	}
}