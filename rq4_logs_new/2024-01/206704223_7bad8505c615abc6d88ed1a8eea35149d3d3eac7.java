package com.coding.challenge.reservoir.sampling;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import com.coding.challenge.ListNode;

class LinkedListRandomNode {
	private final List<Integer> reservoir;
	private final ListNode<Integer> head;
	ListNode<Integer> curr;
	int counter = 0;

	LinkedListRandomNode(ListNode<Integer> head) {
		reservoir = new ArrayList<>();
		this.head = head;
		curr = head;
		for (int i = 0; i < 100 && curr != null; i++) {
			reservoir.add(curr.val);
			curr = curr.next;
		}
	}

	public static void main(String[] args) {
		final ListNode<Integer> head1 = new ListNode<>(1, new ListNode<>(2, new ListNode<>(3)));
		final LinkedListRandomNode listRandNode = new LinkedListRandomNode(head1);
		System.out.println(listRandNode.getRandom());
		System.out.println(listRandNode.getRandom());
		System.out.println(listRandNode.getRandom());
		System.out.println(listRandNode.getRandom());
		System.out.println(listRandNode.getRandom());
	}

	public int getRandom() {
		if (counter >= reservoir.size()) {
			final int randIdx = ThreadLocalRandom.current().nextInt(reservoir.size());
			if (curr == null)
				curr = head;
			reservoir.set(randIdx, curr.val);
			curr = curr.next;
		}
		final int randIdx = ThreadLocalRandom.current().nextInt(reservoir.size());
		return reservoir.get(randIdx);
	}
}