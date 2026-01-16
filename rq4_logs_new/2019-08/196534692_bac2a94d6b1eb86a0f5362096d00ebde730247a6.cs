using System;
using System.Collections;
using System.Collections.Generic;

namespace Collections
{
    internal class MyList<T> : IEnumerable<T>
    {
        private T[] _value = null;

        public int Count
        {
            get
            {
                return _value.Length;
            }
        }

        public T this[int index]
        {
            get
            {
                return _value[index];
            }
        }

        public MyList()
        {
            _value = new T[0];
        }

        public MyList(IEnumerable<T> collection) : this()
        {
            InitializeCollection(collection);
        }

        public void Add(T item)
        {
            IncreaseArrayLength();
            _value[Count - 1] = item;
        }

        public void AddRange(IEnumerable<T> collection)
        {
            InitializeCollection(collection);
        }

        private void IncreaseArrayLength(int item = 1)
        {
            Array.Resize(ref _value, Count + item);
        }

        private void InitializeCollection(IEnumerable<T> collection)
        {
            IEnumerator ie = collection.GetEnumerator();

            int oldCount = Count;
            IncreaseArrayLength(GetCount(collection));

            for (int i = oldCount; i < Count && ie.MoveNext(); i++)
            {
                _value[i] = (T)ie.Current;
            }

            ie.Reset();
        }

        private int GetCount(IEnumerable<T> collection)
        {
            int size = 0;

            IEnumerator<T> enumerator = collection.GetEnumerator();
            while (enumerator.MoveNext())
                size++;

            enumerator.Reset();

            return size;
        }

        public IEnumerator<T> GetEnumerator()
        {
            return ((IEnumerable<T>)_value).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }
    }
}