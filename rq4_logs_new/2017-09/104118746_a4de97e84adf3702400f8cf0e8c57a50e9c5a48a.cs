using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Functions
{
    class Program
    {
        public static void ChangeArgument (int[] array, int Number)
        {
         for (int i = 0; i < array.Length; i++)
            {
                if (array[i] == Number)
                    array[i] = Number - (Number*2);
                Console.WriteLine(array[i]);
            }
          //return array[i];
        }

        static void Main(string[] args)
            {
            int[] arr = { 1, 2, 1, 1, 1 };
            int Number = Convert.ToInt32(Console.ReadLine());
            ChangeArgument(arr, Number);
           // Console.WriteLine(arr[i]);
           Console.ReadKey();

            }
    }
}