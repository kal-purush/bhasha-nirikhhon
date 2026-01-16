package LeetCode;

/**
 * https://leetcode.com/problems/merge-two-sorted-lists/
 * 
 * 21. Merge Two Sorted Lists
 * Easy
 * 17.7K
 * 1.6K
 * Companies
 * 
 * You are given the heads of two sorted linked lists list1 and list2.
 * 
 * Merge the two lists in a one sorted list. The list should be made by splicing together the nodes of the first two lists.
 * 
 * Return the head of the merged linked list.
 * 
 * 
 * 
 * Example 1:
 * 
 * Input: list1 = [1,2,4], list2 = [1,3,4]
 * Output: [1,1,2,3,4,4]
 * 
 * Example 2:
 * 
 * Input: list1 = [], list2 = []
 * Output: []
 * 
 * Example 3:
 * 
 * Input: list1 = [], list2 = [0]
 * Output: [0]
 * 
 * 
 * 
 * Constraints:
 * 
 * The number of nodes in both lists is in the range [0, 50].
 * -100 <= Node.val <= 100
 * Both list1 and list2 are sorted in non-decreasing order.
 */
public class _21_Easy_MergeTwoSortedLists
{

	public static void main(String[] args) throws Exception
	{
		print(mergeTwoLists(new ListNode(1, new ListNode(2, new ListNode(3))), new ListNode(1, new ListNode(3, new ListNode(4)))));
		print(mergeTwoLists(null, null));
		print(mergeTwoLists(null, new ListNode(0)));
	}

	public static void print(ListNode listNode) throws Exception
	{
		System.out.println();
		while(listNode != null)
		{
			System.out.print(listNode.val + "\t");
			listNode = listNode.next;
		}
	}

	public static ListNode mergeTwoLists(ListNode list1, ListNode list2)
	{
		ListNode resultNode = new ListNode(-1);
		ListNode headNode = resultNode;
		if(list1 == null)
		{
			return list2;
		}
		if(list2 == null)
		{
			return list1;
		}
		while(list1 != null && list2 != null)
		{
			if(list1.val < list2.val)
			{
				resultNode.next = list1;
				list1 = list1.next;
				resultNode = resultNode.next;
			}
			else
			{
				resultNode.next = list2;
				list2 = list2.next;
				resultNode = resultNode.next;
			}
		}
		while(list1 != null)
		{
			resultNode.next = list1;
			list1 = list1.next;
			resultNode = resultNode.next;
		}
		while(list2 != null)
		{
			resultNode.next = list2;
			list2 = list2.next;
			resultNode = resultNode.next;
		}
		return headNode.next;
	}

	public static class ListNode
	{
		int val;
		ListNode next;

		ListNode()
		{
		}

		ListNode(int val)
		{
			this.val = val;
		}

		ListNode(int val, ListNode next)
		{
			this.val = val;
			this.next = next;
		}
	}
}