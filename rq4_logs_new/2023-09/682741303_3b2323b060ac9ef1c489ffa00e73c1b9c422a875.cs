using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LeetCode.LinkedList
{
	public class LinkedListCycle
	{
		private ListNode _head;

		public LinkedListCycle()
		{
			_head = null;
		}

		
		public bool HasCycle(ListNode head)
		{
			//Runtime: 100 ms
			//Memory Usage: 44.4 MB
			HashSet<ListNode> visited = new HashSet<ListNode>();
			while (head != null)
			{
				if (visited.Contains(head))
				{
					return true;
				}
				visited.Add(head);
				head = head.Next;
			}
			return false;
		}

		public ListNode DetectCycle(ListNode head)
		{
			//Runtime: 78 ms
			//Memory Usage: 41.7 MB
			HashSet<ListNode> nodeSeen = new HashSet<ListNode>();
			ListNode node = head;

			while (node != null)
			{
				if (nodeSeen.Contains(node))
				{
					return node;
				}
				else
				{
					nodeSeen.Add(node);
					node = node.Next;
				}
			}

			return null;
		}
	}
}