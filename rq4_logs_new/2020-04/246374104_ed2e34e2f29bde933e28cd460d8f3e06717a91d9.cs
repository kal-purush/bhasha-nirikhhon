using System;

namespace Lect12.Task
{
    class UniqueItem
    {
        public int idOfObject;
        public static int id = 0;
        public UniqueItem()
        {
            id++;
            idOfObject = id;
        }
        public UniqueItem(int id)
        {
            UniqueItem.id = id;
            idOfObject = id;
        }
    }
    class Program
    {
        static void Main(string[] args)
        {
            UniqueItem item1 = new UniqueItem();
            UniqueItem item2 = new UniqueItem();
            UniqueItem item3 = new UniqueItem();
            Console.WriteLine(UniqueItem.id);
            UniqueItem item4 = new UniqueItem(6);
            UniqueItem item5 = new UniqueItem();
            Console.WriteLine(UniqueItem.id);
            Console.WriteLine(item1.idOfObject);
            Console.WriteLine(item3.idOfObject);
            Console.WriteLine(item4.idOfObject);
        }
    }
}