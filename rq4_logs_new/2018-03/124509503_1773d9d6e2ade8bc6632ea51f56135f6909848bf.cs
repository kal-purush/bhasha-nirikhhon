namespace DoublyLinkedListLib
{
    using System;
    using System.Collections;

    public class DoublyLinkedList : IEnumerable
    {
        private int?[] data;
        private int?[] next;
        private int?[] previous;
        private int? head;
        private int? tail;
        private int extensionValue = 8;

        public DoublyLinkedList()
        {
            Count = 0;
            data = new int?[extensionValue];
            next = new int?[extensionValue];
            previous = new int?[extensionValue];
        }

        public int Count { get; private set; }

        public bool isEmpty
        {
            get { return Count == 0 ? true : false; }
        }

        public void Add(int value)
        {
            if (Count == 0)
            {
                data[Count] = value;
                next[Count] = null;
                previous[Count] = null;
                head = tail = Count;
            }
            else
            {
                if (Count + 1 > data.Length)
                {
                    Array.Resize(ref data, data.Length + extensionValue);
                    Array.Resize(ref next, next.Length + extensionValue);
                    Array.Resize(ref previous, previous.Length + extensionValue);
                }

                int i = 0;
                while (data[i].HasValue)
                {
                    i++;
                }

                data[i] = value;
                previous[i] = tail;
                next[tail.Value] = tail = i;
            }
            Count++;
        }

        public IEnumerator GetEnumerator()
        {
            for (int i = 0; i < data.Length; i++)
            {
                if (data[i] != null)
                {
                    yield return data[i].Value;
                }
            }
        }

        public void Clear()
        {
            Count = 0;
            data = new int?[1];
            next = new int?[1];
            previous = new int?[1];
        }

        public bool Contains(int value)
        {
            int? current = head;
            while (current != null)
            {
                if (data[current.Value].HasValue)
                {
                    if (value == data[current.Value].Value)
                    {
                        return true;
                    }
                }
                current = next[current.Value];
            }
            return false;
        }

        public int IndexOf(int value)
        {
            int? current = head;
            while (current != null)
            {
                if (data[current.Value].HasValue)
                {
                    if (value == data[current.Value].Value)
                    {
                        return current.Value;
                    }
                }
                current = next[current.Value];
            }
            return -1;
        }

        public void Remove(int value)
        {
            int? current = head;
            while (current != null)
            {
                if (data[current.Value].HasValue)
                {
                    if (value == data[current.Value].Value)
                    {
                        data[current.Value] = null;
                        if (next[current.Value].HasValue && previous[current.Value].HasValue)
                        {
                            int tempNext = next[current.Value].Value;
                            int tempPrev = previous[current.Value].Value;
                            next[tempPrev] = next[current.Value];
                            previous[tempNext] = previous[current.Value];
                        }
                        Count--;
                        break;
                    }
                }
                current = next[current.Value];
            }
        }
    }
}