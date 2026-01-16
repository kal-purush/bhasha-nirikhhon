using System;

namespace ArraysUpdate_Practice
{
    class MinMax_Element
    {
        static void Main(string[] args)
        {
            int[] a = { 99, 95, 93, 89, 87 };
            int min = a[0];
            int max = a[0];

            for(int i = 0; i < a.Length; i++)
            {
                if (a[i] > max)
                {
                    max = a[i];
                    
                }
                if(a[i]<min)
                {
                    min = a[i];
                    
                }

            }
            Console.WriteLine(min);
            Console.WriteLine(max);
        }
    }
}