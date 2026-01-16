using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSharpPractice.main.math_operation
{
    public class Fibonacci
    {
        public void GenerateFibonacciSeries(int n)
        {
            int num1 = 0, num2 = 1;

            Console.WriteLine("Fibonacci series:");

            for (int i = 0; i < n; i++)
            {
                Console.Write(num1 + " ");

                int sum = num1 + num2;
                num1 = num2;
                num2 = sum;
            }
        }
    }
}