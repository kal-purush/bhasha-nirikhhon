namespace Bubble_sort
{ 
    using System;
    using System.Diagnostics;

public class Program
    {
       public static int[] Sort(int[] arr)
        {
            for (int i = 0; i < arr.Length - 1; i++) 
            {
                for (int j = 0; j < arr.Length - i - 1; j++)
                {
                    if (arr[j] > arr[j + 1])
                    {
                        int buf = arr[j]; 

                        arr[j] = arr[j + 1];

                        arr[j + 1] = buf;
                    }
                }
            }

            return arr;
        }

       public static void Main(string[] args)
        {
            var time = Stopwatch.StartNew();

            int[] array = new int[100000];

            Random rand = new Random();     
            
            for (int x = 0; x < array.Length; x++)
            {
                array[x] = rand.Next(1, 100000);
            }

           array = Sort(array);         
           
            for (int i = 0; i < array.Length; i++) 
            {
                Console.WriteLine(array[i]);
            }

            time.Stop();

            Console.WriteLine(time.Elapsed);

            Console.ReadKey();
        }
    }
}