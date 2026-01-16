using System.Collections;
using System.Collections.Generic;

namespace Assignment3Tests
{
    public class GenericListEnumerator<T> : IEnumerator<T>
    {
        private GenericList<T> _list;
        private T _current;
        private int _index;


        public GenericListEnumerator(GenericList<T> genericList)
        {
            if (genericList != null)
            {
                _list = genericList;
                _current = default(T);
                _index = -1;
            }
        }

        public T Current
        {
            get { return _current; }
        }

        object IEnumerator.Current
        {
            get { return Current; }
        }

        public bool MoveNext()
        {
            if (++_index < _list.Count)
            {
                _current = _list.GetElement(_index);
                return true;
            }
            return false;
        }

        public void Dispose() { }

        public void Reset()
        {
            _index = -1;
        }
    }
}