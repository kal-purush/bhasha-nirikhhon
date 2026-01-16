using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace LeetCode.LinkedList
{
	public class IntersectionTwoLinkedLists
	{

		//Intersection of Two Linked Lists
		//Given the heads of two singly linked-lists headA and headB, return the node at which the two lists intersect.
		//If the two linked lists have no intersection at all, return null.

		//Example 1:
		//Input: intersectval = 8, listA = [4,1,8,4,5], listB = [5,6,1,8,4,5], skipA = 2, skipB = 3
		//Output: Intersected at '8'
		//Explanation: The intersected node's value is 8 (note that this must not be 0 if the two lists intersect).
		//From the head of A, it reads as [4,1,8,4,5]. From the head of B, it reads as [5,6,1,8,4,5].
		//There are 2 nodes before the intersected node in A; There are 3 nodes before the intersected node in B.
        // - Note that the intersected node's value is not 1 because the nodes with value 1 in A and B (2nd node in A and 3rd node in B) are different node references.
		// In other words, they point to two different locations in memory, while the nodes with value 8 in A and B (3rd node in A and 4th node in B) point to the same location in memory.
		public ListNode GetIntersectionNode(ListNode headA, ListNode headB)
		{
			//Runtime: 116 ms
			//Memory Usage: 53.9 MB
			HashSet<ListNode> visited = new HashSet<ListNode>();
			
			while (headB != null)
			{
				visited.Add(headB);
				headB = headB.next;
			}
			while(headA != null)
			{
				if(visited.Contains(headA)) 
					return headA;
				
				headA = headA.next;
			}
			return null;
		}
	}
}