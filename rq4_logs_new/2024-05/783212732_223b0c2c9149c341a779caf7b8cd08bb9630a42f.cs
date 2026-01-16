using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CycledList
{
    internal class CycledLinkedList<T>
    {
        public LinkedList<T> Data { get; }

        public CycledLinkedList(T[] elements)
        {
            Data = new LinkedList<T>(elements);
        }

        public void AddFirst(T item)
        {
            Data.AddFirst(item);
        }

        public void AddLast(T item)
        {
            Data.AddLast(item);
        }

        public LinkedListNode<T> Next(LinkedListNode<T> current)
        {
            LinkedListNode<T> el = current.Next;
            if (el == null)
            {
                el = Data.First;
            }
            return el;
        }

        public LinkedListNode<T> Previous(LinkedListNode<T> current)
        {
            LinkedListNode<T> el = current.Previous;
            if (el == null)
            {
                el = Data.Last;
            }
            return el;
        }
    }
}