using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataStructures
{
	class Heap<T>
	{
		public List<T> Values { get; set; }
		public Comparison<T> Comp { get; set; }

		public Heap(Comparison<T> comp)
		{
			Comp = comp;
		}

		public Heap(Comparison<T> comp, IEnumerable<T> values)
		{
			Comp = comp;
			Values = new List<T>();
			foreach (T item in values)
			{
				Push(item);
			}
		}

		/// <summary>
		/// Adds a value to the heap.
		/// </summary>
		/// <param name="data"></param>
		public void Push(T data)
		{
			int idx = Values.Count;
			Values.Add(data);
			while (idx < 1 && Comp(Values[idx], Values[(idx - 1) / 2]) == 1)
			{
				T temp = Values[idx];
				Values[idx] = Values[(idx - 1) / 2];
				Values[(idx - 1) / 2] = temp;
				idx = (idx - 1) / 2;
			}
		}

		/// <summary>
		/// Removes the highest priority object from the heap.
		/// </summary>
		/// <returns></returns>
		public T Pop()
		{
			T val = Values[0];
			int idx = 0;
			while (Values.Count >= idx * 2)
			{
				if (Values.Count != idx * 2 && Comp(Values[idx * 2 + 2], Values[idx * 2 + 1]) == 1)
				{
					Values[idx] = Values[idx * 2 + 2];
					idx = idx * 2 + 2;
				}
				else
				{
					Values[idx] = Values[idx * 2 + 1];
					idx = idx * 2 + 1;
				}
			}
			Values.RemoveAt(idx);
			return val;
		}
	}
}