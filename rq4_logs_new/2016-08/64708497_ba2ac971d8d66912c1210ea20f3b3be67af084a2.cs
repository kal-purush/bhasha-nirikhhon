using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mathmagician
{
    class Fibonacci
    {
        public void PrintFib(int numbers1)
        {
            int[] intArray = new int[numbers1];
            {
                intArray[0] = 1;
                intArray[1] = 1;
                for (var i = 2; i < numbers1; i++)
                {
                    intArray[i] = intArray[i - 2] + intArray[i - 1];
                }
                foreach (int item in intArray)
                {
                    Console.Write("{0}, ", item);
                }
                Console.ReadLine();
            }
        }

    }
}