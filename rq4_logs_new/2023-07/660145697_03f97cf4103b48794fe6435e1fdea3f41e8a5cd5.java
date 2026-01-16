package rish.leets.grind.easy;

/*
 * Grind 75 : Week 1
 * 
 * Problem #: 21
 * Problem link : https://leetcode.com/problems/merge-two-sorted-lists/
 * 
 * @author Rishabh Soni
 *
 */
public class MergeTwoSortedList {

	/*
	 * Definition for singly-linked list.
	 */
	class ListNode {

		int val;
		ListNode next;

		ListNode() {
		}

		ListNode(int val) {
			this.val = val;
		}

		ListNode(int val, ListNode next) {
			this.val = val;
			this.next = next;
		}

	}

	public ListNode mergeTwoLists(ListNode list1, ListNode list2) {

		if (list1 == null && list2 == null) {
			return null;
		}

		if (list1 == null) {
			return list2;
		}

		if (list2 == null) {
			return list1;
		}

		ListNode head = null;

		// iterate thru both lists
		ListNode iter1 = list1;
		ListNode iter2 = list2;
		ListNode iter3 = null;

		while (iter1 != null && iter2 != null) {

			int newVal = Math.min(iter1.val, iter2.val);
			ListNode temp = new ListNode(newVal);

			if (head == null) {
				head = temp;
				iter3 = head;
			} else {
				iter3.next = temp;
				iter3 = iter3.next;
			}

			if (iter1.val < iter2.val) {
				iter1 = iter1.next;
			} else {
				iter2 = iter2.next;
			}
		}

		if (iter1 == null && iter2 != null) {
			iter3.next = iter2;
		}

		if (iter1 != null && iter2 == null) {
			iter3.next = iter1;
		}

		return head;
	}

}