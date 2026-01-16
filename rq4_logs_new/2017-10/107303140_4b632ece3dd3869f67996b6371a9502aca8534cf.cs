using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Assignment2Tests
{
    public class GenericList<X> : IGenericList<X>
    {
        private X[] _internalStorage;
        private int _maxIndex;
        private int _currentIndex;

        public GenericList()
        {
            _internalStorage = new X[4];
            _maxIndex = 3;
            _currentIndex = 0;
        }

        public GenericList(int initialSize)
        {
            if (initialSize >= 1)
            {
                _internalStorage = new X[initialSize];
                _maxIndex = initialSize - 1;
                _currentIndex = 0;
            }

            else
            {
                initialSize *= -1;
                _internalStorage = new X[initialSize];
                _maxIndex = initialSize - 1;
                _currentIndex = 0;
            }

        }

        public int Count
        {
            get { return _currentIndex; }

        }

        public void Add(X item)
        {
            if (item != null)
            {

                if (_currentIndex > _maxIndex)
                {
                    Array.Resize(ref _internalStorage, (_maxIndex + 1) * 2);
                    _maxIndex = (_maxIndex + 1) * 2 - 1;
                }
                _internalStorage[_currentIndex++] = item;


            }
        }

        public bool RemoveAt(int index)
        {

            if (index >= _currentIndex || index < 0)
            {
                throw new IndexOutOfRangeException("Index number is too big/small for this array!");
            }

            for (int i = index; i < _currentIndex - 1; i++)
            {
                _internalStorage[i] = _internalStorage[i + 1];
            }

            Array.Resize(ref _internalStorage, _maxIndex);
            _maxIndex -= 1;
            _currentIndex -= 1;

            return true;

        }

        public bool Remove(X item)
        {
            int i;

            if (item != null)
            {
                for (i = 0; i < _currentIndex; i++)
                    if (_internalStorage[i].Equals(item))
                        return RemoveAt(i);

            }

            return false;


        }

        public X GetElement(int index)
        {
            if (index >= 0 && index < _currentIndex)
            {
                return _internalStorage[index];
            }
            else
            {
                throw new IndexOutOfRangeException("There is no element at the given index!");
            }
        }

        public int IndexOf(X item)
        {
            if (item != null)
            {
                for (int i = 0; i < _currentIndex; i++)
                    if (_internalStorage[i].Equals(item))
                        return i;
            }

            return -1;

        }

        public void Clear()
        {
            Array.Resize(ref _internalStorage, 0);
            _maxIndex = -1;
            _currentIndex = 0;

        }

        public bool Contains(X item)
        {
            
            for (int i = 0; i < _currentIndex; i++)
                if (_internalStorage[i].Equals(item))
                    return true;
            

            return false;
        }
    }
}