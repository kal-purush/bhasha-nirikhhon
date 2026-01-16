using System;

namespace BinarySearch
{
    public class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Binary Search Method");
        }
        public static int BinarySearchIndex(int[] sortedArray, int value)
        {
            int min = 0;
            int max = sortedArray.Length - 1;
            int mid = (min + max) / 2;

            while (min <= max)
            {
                mid = (max + min) / 2;

                if (sortedArray[mid] == value)
                {
                    return mid;
                }
                else if (sortedArray[mid] < value)
                {
                    min = mid + 1;
                }
                else
                {
                    max = mid - 1;
                }
            }
            return -1;
        }
    }
}