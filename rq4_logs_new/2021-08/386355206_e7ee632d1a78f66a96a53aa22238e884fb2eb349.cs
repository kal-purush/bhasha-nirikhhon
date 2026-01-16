using System;
using System.Collections;
using System.Collections.Generic;

namespace ArrayList_ListGen
{
    class Program
    {
        static void Main()
        {
            ArrayList arrayList = new ArrayList();

            // Boxing.
            arrayList.Add(1);
            arrayList.Add((object)2);


            // Unboxing
            int i1 = (int)arrayList[0];

            for (int i = 0; i < arrayList.Count; i++)
                Console.WriteLine((int)arrayList[i]);


            Console.WriteLine(new string('-', 3));


            List<int> list = new List<int>();

            // упаковки нет.
            list.Add(3);
            list.Add(4);

            // Распаковки нет.
            int i3 = list[0];

            for (int i = 0; i < list.Count; i++)
                Console.WriteLine(list[i]);

            // Delay.
            Console.ReadKey();
        }
    }
}