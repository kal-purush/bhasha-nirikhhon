using System;

namespace Lect12.Task
{
    class UniqueItem
    {
        public static int id = 0;
        public UniqueItem()
        {
            id++;
        }
        public UniqueItem(int id)
        {
            UniqueItem.id = id;
        }
    }
    class Program
    {
        static void Main(string[] args)
        {
            UniqueItem item1 = new UniqueItem();
            UniqueItem item2 = new UniqueItem();
            UniqueItem item3 = new UniqueItem();
            UniqueItem item4 = new UniqueItem(6);
            UniqueItem item5 = new UniqueItem();
            Console.WriteLine(UniqueItem.id);
        }
    }
}