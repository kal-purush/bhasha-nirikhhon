using System;

namespace ConsoleApplication1
{
    class ArrayDemo1
    {
        static void Main()
        {
           // int[] arr = new int[5];

            int[] arr; //declaring an array
            arr = new int[5]; // 
            arr[0] = 41; // i itializing an array
            arr[1] = 20;
            arr[2] = 21;
            arr[3] = 12;
            arr[4] = 10;
           
            for (int i = 0; i < arr.Length; i++)
            {
                Console.WriteLine(arr[i]);
            }
            Console.ReadLine();
        }
    }
}