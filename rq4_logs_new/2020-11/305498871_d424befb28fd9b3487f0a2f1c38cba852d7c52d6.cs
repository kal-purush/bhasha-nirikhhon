using System;

namespace MyDynamicArray
{
    public class DynamicArrayOfString
    {
        private const int _defaultSize = 4;
        private string[] _stringArray;
        private int _length;

        public DynamicArrayOfString() : this(_defaultSize)
        {
        }

        public DynamicArrayOfString(int initialSize)
        {
            //Length = 0;
            Size = initialSize;
            _stringArray = new string[initialSize];
        }

        public int Size { get; private set; }
        public int Length
        {
            get
            {
                return GetLength();
            }
            private set
            {
                _length = GetLength() + value;
            }
        }

        public string this[int index]
        {
            get
            {
                if (index < _stringArray.Length)
                {
                    return _stringArray[index];
                }
                else
                {
                    return "Index is put of range!";
                }
            }
            set
            {
                if (index < _stringArray.Length)
                {
                    _stringArray[index] = value;
                }
                else
                {
                    Console.WriteLine("Index is out of range!");
                }
            }
        }

        public void Add(string stringItem)
        {
            if (Size > Length)
            {
                _stringArray[Length++] = stringItem;
            }
            else
            {
                _stringArray = ResizeArray();
                Size = _stringArray.Length;
                this.Add(stringItem);
            }
        }
        private string[] ResizeArray()
        {
            string[] newSimpleArray = new string[_stringArray.Length * 2];

            for (int i = 0; i < _stringArray.Length; i++)
            {
                newSimpleArray[i] = _stringArray[i];
            }
            return newSimpleArray;
        }
        private int GetLength()
        {
            int count = 0;
            foreach (var str in _stringArray)
            {
                if (str != null)
                {
                    count++;
                }
            }
            return count;
        }
    }

}