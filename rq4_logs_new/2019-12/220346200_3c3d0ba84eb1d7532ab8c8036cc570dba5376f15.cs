using System;

namespace Graph
{
	public class NodeStack<T>
	{
		public T data;
		public NodeStack<T> next;

		public NodeStack(T newData)
		{
			data = newData;
			next = null;
		}
	}

	public class StackList<T>
	{
		private NodeStack<T> head;
		private int size;

		public StackList()
		{
			head = null;
			size = 0;
		}

		private void Add(NodeStack<T> node)
		{
			NodeStack<T> current = node;
			if (head == null)
			{
				current.next = null;
			}
			else
			{
				current.next = head;
			}
			head = node;
			size++;
		}

		public void Push(T data)
		{
			NodeStack<T> node = new NodeStack<T>(data);
			Add(node);
		}

		public void Pop()
		{
			if (head == null)
			{
				Console.WriteLine("stack is empty");
			}
			else
			{
				head = head.next;
			}
			size--;
		}

		public bool IsEmpty()
		{
			return size == 0;
		}

		public int GetSize()
		{
			return size;
		}
	}
}