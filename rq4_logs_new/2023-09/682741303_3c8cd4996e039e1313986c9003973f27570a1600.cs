using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace LeetCode.LinkedList
{
	public class Node
	{
		public int val;
		public Node prev;
		public Node next;
		public Node child;
	}

	//430. Flatten a Multilevel Doubly Linked List
	//You are given a doubly linked list, which contains nodes that have a next pointer, a previous pointer, and an additional child pointer. This child pointer may or may not point to a separate doubly linked list, also containing these special nodes.
	//These child lists may have one or more children of their own, and so on, to produce a multilevel data structure as shown in the example below.
	//Given the head of the first level of the list, flatten the list so that all the nodes appear in a single-level, doubly linked list.Let curr be a node with a child list.
	//The nodes in the child list should appear after curr and before curr.next in the flattened list.
	//Return the head of the flattened list.The nodes in the list must have all of their child pointers set to null.
	public class FlattenMultilevelDoublyLinkedList
	{
		public Node Flatten(Node head)
		{
			//Runtime: 86 ms
			//Memory: 38.72 MB
			Traverse(head);
			return head;
		}

		private Node Traverse(Node head)
		{
			Node node = new Node();
			Node tempNode, parentNode;
			while (node != null && (node.child != null || node.next != null))
			{
				if (node.child != null)
				{
					parentNode = node;
					tempNode = node.next;
					node.next = node.child;
					node.child = null; // Setting the child node to null.
					node = node.next; // Moving the curr node to child node.
					node.prev = parentNode; // Setting parent node as previous node.
					node.next = Traverse(node);
					node.next = tempNode;
					if (tempNode != null)
					{
						tempNode.prev = node; // Setting current node as tempNode's prev node.
					}
				}
				if (node.next != null)
				{
					node = node.next;
				}
			}
			return node;
		}
	}
}