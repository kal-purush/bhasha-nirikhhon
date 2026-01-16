using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LeetCode.LinkedList
{
	public class OddEvenLinkedList
	{
		//328. Odd Even Linked List
		//Given the head of a singly linked list, group all the nodes with odd indices together followed by the nodes with even indices, and return the reordered list.
		//The first node is considered odd, and the second node is even, and so on.
		//Note that the relative order inside both the even and odd groups should remain as it was in the input.
		//You must solve the problem in O(1) extra space complexity and O(n) time complexity.
		public ListNode OddEvenList(ListNode head)
		{
			//Runtime: 74 ms
			//Memory Usage: 40 MB
			if (head == null) return null;

			ListNode odd = head;
			ListNode even = head.next, evenHead = even;
			while (even != null && even.next != null)
			{
				odd.next = even.next;
				odd = odd.next;
				even.next = odd.next;
				even = even.next;
			}
			odd.next = evenHead;
			return head;
		}
	}
}