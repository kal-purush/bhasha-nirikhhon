using System.Collections.Generic;
using System.Collections;
using System;

namespace Task1
{
    public partial class List<T>
    {
        class ListEnumerator : IEnumerator<T>
        {
            private readonly Node head;
            private Node current;

            public ListEnumerator(Node head)
            {
                this.head = head;
                current.Next = head;
            }

            public bool MoveNext()
            { 
                current = current.Next;
                if (current == null)
                {
                    return false;
                }
                return true;
            }

            public void Reset() => current.Next = head;

            public T Current => current.Value;

            object IEnumerator.Current => current;

            public void Dispose()
            {
                throw new Exception();
            }
        }
    }
}