using System;

namespace Task3
{
    class HashTable : InterfaceHashTable
    {
        public HashTable(int maxSize)
        {
            this.maxSize = maxSize;
            this.hashTable = new List[maxSize];
            for (int i = 0; i < maxSize; i++)
                hashTable[i] = new List();
        }

        private int maxSize;
        private List[] hashTable;

        public int Hash(string inputString)
        {
            int primeNumber = 67;
            int degree = 1;
            int length = inputString.Length;
            int result = 0;
            for (int i = 0; i < length; i++)
            {
                result = ((inputString[i] * degree) % maxSize + result) % maxSize;
                degree = (degree * primeNumber) % maxSize;
            }
            return result;
        }

        public void AddElement(string value)
        {
            int index = Hash(value);
            hashTable[index].Add(value);
        }

        public void RemoveElement(string value)
        {
            int index = Hash(value);
            hashTable[index].RemoveElement(value);
        }

        public bool IsExist(string value)
        {
            int index = Hash(value);
            return hashTable[index].IsExist(value);
        }

        public void PrintListOfIndex(int index)
        {
            hashTable[index].PrintList();
        }

        public void PrintHashTable()
        {
            for (int i = 0; i < maxSize; i++)
            {
                PrintListOfIndex(i);
            }
        }

        public void PrintStatistic()
        {
            int numberOfElements = 0;
            double average = 0;
            int emptyCells = 0;
            for (int i = 0; i < maxSize; i++)
            {
                numberOfElements = numberOfElements + hashTable[i].Length;
                average = (double)numberOfElements / (double)maxSize;
                if (hashTable[i].Length == 0)
                    emptyCells++;
            }
            Console.WriteLine("Number elements of list: " + numberOfElements);
            Console.WriteLine("Average length: " + average);
            Console.WriteLine("Empty cells: " + emptyCells);
        }
    }
}