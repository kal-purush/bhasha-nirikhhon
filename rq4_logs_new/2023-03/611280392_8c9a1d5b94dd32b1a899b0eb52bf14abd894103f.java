package LeetCode;

/**
 * https://leetcode.com/problems/add-two-numbers/
 * 
 * 2. Add Two Numbers
 * Medium
 * 
 * You are given two non-empty linked lists representing two non-negative integers.
 * The digits are stored in reverse order, and each of their nodes contains a single digit.
 * Add the two numbers and return the sum as a linked list.
 * 
 * You may assume the two numbers do not contain any leading zero, except the number 0 itself.
 * 
 * 
 * 
 * Example 1:
 * 
 * Input: l1 = [2,4,3], l2 = [5,6,4]
 * Output: [7,0,8]
 * Explanation: 342 + 465 = 807.
 * 
 * Example 2:
 * 
 * Input: l1 = [0], l2 = [0]
 * Output: [0]
 * 
 * Example 3:
 * 
 * Input: l1 = [9,9,9,9,9,9,9], l2 = [9,9,9,9]
 * Output: [8,9,9,9,0,0,0,1]
 * 
 * 
 * 
 * Constraints:
 * 
 * The number of nodes in each linked list is in the range [1, 100].
 * 0 <= Node.val <= 9
 * It is guaranteed that the list represents a number that does not have leading zeros.
 */
public class _2_Medium_AddTwoNumbers
{
	public ListNode addTwoNumbers(ListNode l1, ListNode l2)
	{
		ListNode resultNode = new ListNode(0);
		ListNode dummyHeadNode = resultNode;
		int carry = 0;
		while(l1 != null || l2 != null || carry != 0)
		{
			int resultVal = carry;
			if(l1 != null)
			{
				resultVal = resultVal + l1.val;
				l1 = l1.next;
			}
			if(l2 != null)
			{
				resultVal = resultVal + l2.val;
				l2 = l2.next;
			}
			carry = resultVal / 10;
			resultVal = resultVal % 10;
			resultNode.next = new ListNode(resultVal);
			resultNode = resultNode.next;
		}
		return dummyHeadNode.next;
	}

	public class ListNode
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