using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataStructures
{
	class PriorityQueue<T>
	{
		public Heap<Tuple<T, int>> Heap { get; set; }

		public PriorityQueue()
		{
			Heap = new Heap<Tuple<T, int>>((t, u) => t.Item2 > u.Item2 ? 1 : t.Item2 < u.Item2 ? -1 : 0);
		}

		public void Insert(T value, int priority = 0)
		{
			Heap.Push(Tuple.Create(value, priority));
		}

		public T Pop()
		{
			return Heap.Pop().Item1;
		}

		public T Peek()
		{
			return Heap.Values.Count > 0 ? Heap.Values[0].Item1 : default(T);
		}
	}
}