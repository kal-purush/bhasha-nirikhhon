using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SortingAlgorithm
{
    class MainApp
    {
        static void Main( string[] args )
        {
            int[] arg = { 12, 18, 9, 80, 16, 100, 8, 18, 30, 16, 7, 1, 5, 4 };
            Print();

            QuickSort.AscendingSort(arg, 0, arg.Length - 1);
            Print();


            void Print()
            {
                foreach( int i in arg )
                {
                    Console.Write($"{i} ");
                }
                Console.WriteLine("\n");
            }
        }
    }
}