using System;
using System.Text;
using System.Threading;

namespace Factorial
{
    class Program
    {
        static void Main(string[] args)
        {            
            //Factorial(10);
            //Sum(10);
            new Thread(() => Factorial(10)).Start();
            new Thread(() => Sum(10)).Start();
            Console.WriteLine("Завершение основного потока");
            //Console.ReadKey();
        }
        static void Factorial(int n)
        {
            StringBuilder _string = new StringBuilder();
            if (n <= 1 || n < 0) {
                Console.WriteLine($"Factorial {n} = 1");
                return;
            }
            int factorial=1;
            _string.Append($"Factorial {n} = 1");
            for (int i = 2; i<=n; i++)
            {
                _string.Append($" * {i}");
                factorial *= i;
            }
            _string.Append($" = {factorial}");
            Console.WriteLine(_string);

        }
        static void Sum(int n)
        {
            StringBuilder _string = new StringBuilder();
            int sum = 0;
            _string.Append($"Summ {n} = ");
            for (int i = 1; i <= n; i++)
            {
                if (i == 1) _string.Append($"{i}");
                else _string.Append($" + {i}");
                sum += i;
            }
            _string.Append($" = {sum}");
            Console.WriteLine(_string);
            //return sum;
        }
    }
}