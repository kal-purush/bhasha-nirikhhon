
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace labsheet
{
    class Program1
    {
        static void Main(String[] args)
        {
            Console.Write("Enter the Number:- ");
            int n = Convert.ToInt32(Console.ReadLine());
            check_fib(n);
            Console.ReadKey();
        }
        static void check_fib(int n)
        {
            int a = -1, b = 1, c;
            bool flag = false;
            while (true)
            {
                c = a + b;
                a = b;
                b = c;
                if (c == n)
                    flag = true;
                if (c > n)
                    break;
            }
            Console.WriteLine(flag ? n + " is a fibonacci series number" : n + " is not a fibonacci series number");

        }
    }
}