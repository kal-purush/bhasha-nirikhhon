using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSharpPractice.main.math_operation
{
    public class CountSpaces
    {
        public void countProbils(string str)
        {
            int length = str.Length;
            for (int i = 0; i < length; i++)
            {
                if (str[i] == ' ')
                    Console.WriteLine("Space found by index " + i);
            }
        }

    }
}