package org.geeksforgeeks.linkedlists;

import java.security.InvalidParameterException;
import java.util.Iterator;

/**
 * Given a linked list and two integers M and N. Traverse the linked list such
 * that you retain M nodes then delete next N nodes, continue the same till end
 * of the linked list.
 * 
 * @author Shiva
 *
 * @param <Item>
 */
public class DeleteNnodesAfterMnodes_49<Item extends Comparable<Item>> implements
		Iterable<Item> {

	private Node head = null;

	private class Node {
		public Item data;
		public Node next;

		public Node(Item data) {
			this.data = data;
			this.next = null;
		}
	}

	public void addItem(Item item) {
		Node temp = new Node(item);
		temp.next = head;
		head = temp;
	}

	public Node deleteNnodesAfterMnodes(Node heaNode, int m, int n) {
		if (m == 0)
			throw new InvalidParameterException("m should not be 0");
		if (n == 0) // no elements deleted
			return head;
		int count = 1; // headNode already pointed
		boolean dlt = false;
		Node tmp = heaNode;
		while (tmp != null && tmp.next != null) {
			// deletes nodes
			if (dlt) {
				tmp.next = tmp.next.next;
				if (++count == n) {
					dlt = false;
					count = 0;
				}
			}
			// retains nodes
			else {
				tmp = tmp.next;
				if (++count == m) {
					dlt = true;
					count = 0;
				}
			}
		}
		return heaNode;
	}

	@Override
	public Iterator<Item> iterator() {
		return new Iterator<Item>() {
			Node current = head;

			@Override
			public boolean hasNext() {
				return current != null;
			}

			@Override
			public Item next() {
				Node temp = current;
				current = current.next;
				return temp.data;
			}
		};
	}

	public static void main(String[] args) {

		DeleteNnodesAfterMnodes_49<Integer> llist = new DeleteNnodesAfterMnodes_49<Integer>();
		llist.addItem(9);
		llist.addItem(8);
		llist.addItem(7);
		llist.addItem(6);
		llist.addItem(5);
		llist.addItem(4);
		llist.addItem(3);
		llist.addItem(2);
		llist.addItem(1);

		System.out.print("\n Linked list: ");
		for (Integer i : llist)
			System.out.print(i + " ");

		System.out
				.print("\n Linked list deleting 2 nodes after 2 node conitinuously: ");
		llist.deleteNnodesAfterMnodes(llist.head, 2, 2);
		for (Integer i : llist)
			System.out.print(i + " ");
	}

}