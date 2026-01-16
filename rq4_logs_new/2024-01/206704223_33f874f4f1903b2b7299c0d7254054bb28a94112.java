package com.coding.challenge.linked.list;

import com.coding.challenge.ListNode;

class ReverseLinkedListII {
	ReverseLinkedListII() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final ListNode<Integer> head1 = new ListNode<>(1,
				new ListNode<>(2, new ListNode<>(3, new ListNode<>(4, new ListNode<>(5)))));
		assert "14325".equals(reverseBetween(head1, 2, 4).toString());

		final ListNode<Integer> head2 = new ListNode<>(5);
		assert "5".equals(reverseBetween(head2, 1, 1).toString());

		final ListNode<Integer> head3 = new ListNode<>(1,
				new ListNode<>(2, new ListNode<>(3, new ListNode<>(4, new ListNode<>(5)))));
		assert "21345".equals(reverseBetween(head3, 1, 2).toString());

		final ListNode<Integer> head4 = new ListNode<>(1,
				new ListNode<>(2, new ListNode<>(3, new ListNode<>(4, new ListNode<>(5)))));
		assert "12354".equals(reverseBetween(head4, 4, 5).toString());

		final ListNode<Integer> head5 = new ListNode<>(1,
				new ListNode<>(2, new ListNode<>(3, new ListNode<>(4, new ListNode<>(5)))));
		assert "54321".equals(reverseBetween(head5, 1, 5).toString());

		final ListNode<Integer> head6 = new ListNode<>(1,
				new ListNode<>(2, new ListNode<>(3, new ListNode<>(4, new ListNode<>(5)))));
		assert "12543".equals(reverseBetween(head6, 3, 5).toString());

		final ListNode<Integer> head7 = new ListNode<>(1,
				new ListNode<>(2, new ListNode<>(3, new ListNode<>(4, new ListNode<>(5)))));
		assert "32145".equals(reverseBetween(head7, 1, 3).toString());

		final ListNode<Integer> head8 = new ListNode<>(1,
				new ListNode<>(2, new ListNode<>(3, new ListNode<>(4, new ListNode<>(5)))));
		assert "12345".equals(reverseBetween(head8, 1, 1).toString());
	}

	static ListNode<Integer> reverseBetween(ListNode<Integer> head, int left, int right) {
		ListNode<Integer> beforeLeft = null;
		ListNode<Integer> l = null;
		ListNode<Integer> curr = new ListNode<>(-1, head);
		for (int i = 1; i < left; i++)
			curr = curr.next;

		beforeLeft = curr;
		curr = curr.next;
		l = curr;

		ListNode<Integer> prev = curr;
		ListNode<Integer> next = null;
		curr = curr.next;
		for (int j = left + 1; j <= right; j++) {
			next = curr.next;
			curr.next = prev;
			prev = curr;
			curr = next;
		}
		beforeLeft.next = prev;
		l.next = curr;
		return left == 1 ? prev : head;
	}
}