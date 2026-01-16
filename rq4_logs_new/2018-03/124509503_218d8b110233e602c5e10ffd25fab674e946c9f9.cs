using System;

namespace MergeSort
{
    class EntryPoint
    {
        static void Main(string[] args)
        {
            int [] array = GetArray();
            Print(array);
            MergeSortLib.MergeSort.Sort(array, 0, array.Length-1);
            Print(array);
        }

        static void Print(int[] array)
        {
            for (int i = 0; i < array.Length; i++)
            {
                Console.Write("{0}, ", array[i]);
            }
            Console.WriteLine();
        }

        static int [] GetArray()
        {
            Random r = new Random();
            int [] a = new int[r.Next(1000)];
            for(int i=0; i<a.Length; i++)
            {
                a[i] = r.Next(0, 1000);
            }
            return a;
        }
    }
}