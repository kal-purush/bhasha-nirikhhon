using System;

namespace BinarySearch
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            int[] array = { 4, 8, 15, 16, 23, 42 };
            int searchkey = 15;
            BinarySearch(array, searchkey);
        }

        static int BinarySearch(int[] arr, int searchkey)
        {
            int left = 0;
            int right = arr.Length;
            int middle = left + right / 2;

            while (left <= right)
            {
                if (arr[middle] < searchkey)
                {
                    left = middle + 1;
                }

                if(arr[middle] > searchkey)
                {
                    right = middle - 1;
                }
                else
                {
                    return middle;
                }
            }

            return middle;
        }
    }
}