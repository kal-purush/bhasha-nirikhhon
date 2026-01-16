using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Assignment1Tests;

namespace Task_1
{
    public class IntegerList : IIntegerList
    {
        private int[] _internalStorage;
        private int _maxIndex;
        private int _currentIndex;

        public IntegerList()
        {
            _internalStorage = new int[4];
            _maxIndex = 3;
            _currentIndex = 0;
        }

        public IntegerList(int initialSize)
        {
            if (initialSize >= 1)
            {
                _internalStorage = new int[initialSize];
                _maxIndex = initialSize - 1;
                _currentIndex = 0;
            }

            else
            {
                initialSize *= -1;
                _internalStorage = new int[initialSize];
                _maxIndex = initialSize - 1;
                _currentIndex = 0;

            }

        }

        public int Count
        {
            get { return _currentIndex; }

        }

        public void Add(int item)
        {
            if (_currentIndex > _maxIndex)
            {
                Array.Resize(ref _internalStorage, (_maxIndex + 1) * 2);
                _maxIndex = (_maxIndex + 1) * 2 - 1;
            }
            _internalStorage[_currentIndex++] = item;

        }

        public bool RemoveAt(int index)
        {

            if (index > _maxIndex || index < 0)
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

        public bool Remove(int item)
        {
            int i;
            for (i = 0; i < _currentIndex; i++)
                if (_internalStorage[i] == item)
                    return RemoveAt(i);

            return false;




        }

        public int GetElement(int index)
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

        public int IndexOf(int item)
        {

            for (int i = 0; i < _currentIndex; i++)
                if (_internalStorage[i] == item)
                    return i;

            return -1;

        }

        public void Clear()
        {
            Array.Resize(ref _internalStorage, 0);
            _maxIndex = -1;
            _currentIndex = 0;

        }

        public bool Contains(int item)
        {
            for (int i = 0; i < _currentIndex; i++)
            {
                if (_internalStorage[i] == item)
                {
                    return true;
                }
            }
            return false;
        }


    }

}