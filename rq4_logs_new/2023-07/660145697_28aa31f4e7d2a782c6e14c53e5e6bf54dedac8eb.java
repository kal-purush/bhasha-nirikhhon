package rish.leets.grind75;

/*
 * Grind 75 : Week 2
 * 
 * Problem #: 876
 * Problem link : https://leetcode.com/problems/middle-of-the-linked-list/
 * 
 * @author Rishabh Soni
 *
 */
public class MiddleOfLinkedList {

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

	public ListNode middleNode(ListNode head) {

		if (head.next == null) {
			return head;
		}

		/* 
		* Use two pointers approach
		* Pointer i moves one node ahead at a time
		* Pointer j moves two nodes ahead at a time
		*/
		ListNode i = head;
		ListNode j = head;

		do {
			if (j.next == null) {
				return i;
			}
			if (j.next.next == null) {
				return i.next;
			}
			i = i.next;
			j = j.next.next;
		} while (i != null && j != null);

		return head;
	}

}