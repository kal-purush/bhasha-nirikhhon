using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Searching
{
    internal class Program
    {
        static bool LinerSearch(int[] arr , int target)
        {
            for (int i = 0; i < arr.Length; i++)
            {
                if (arr[i] == target)
                {
                    Console.WriteLine($"{target} is at {i}");
                    return true;
                }
            }
            Console.WriteLine($"{target} is Not present in array");
            return false;
        }


        static bool BainarySearch(int[] arr, int target, int s, int e)
        {
            int mid;
            while (s < e)
            {
                mid = (s + e) / 2;

                if (arr[mid] == target)
                {
                    Console.WriteLine($"{target} is at {mid}");
                    return true;
                }
                else if (arr[mid] > target)
                {
                    e = mid - 1;
                }
                else
                {
                    s = mid + 1;
                }
            }
            Console.WriteLine($"target is Not present");
            return false;

        }
        static void Main(string[] args)
        {

            //bainary search
            Console.WriteLine("Bainary search :");
            int[] a = { 1, 2, 3, 4, 5, 6, 7, 8, 9 };
            foreach (int i in a)
            {
                Console.Write(i + " ");

            }
            Console.WriteLine();
            BainarySearch(a, 3, 0, a.Length);
            Console.WriteLine();

            Console.WriteLine("Bainary search :");

            a = new int[]{ 1, 2, 3, 4, 5, 6, 7, 8, 9 };
            foreach (int i in a)
            {
                Console.Write(i + " ");

            } Console.WriteLine();
            LinerSearch(a, 3);
        }
    }
}