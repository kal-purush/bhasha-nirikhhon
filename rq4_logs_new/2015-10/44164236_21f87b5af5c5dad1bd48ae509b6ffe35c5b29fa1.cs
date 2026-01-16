using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;

namespace Primalitate
{
    class Program
    {
        private static int ExtractInt()
        {
            Console.WriteLine("Introduceti un numar natural: ");

            string line;
            line = Console.ReadLine();

            int n = 0;

            try
            {
                n = Int32.Parse(line);
            }
            catch (Exception)
            {
                Console.WriteLine("Oops! Something went wrong. This is really embarassing! We'll do our best to address this issue in a future version of our code.");
            }

            return n;
        }
        static void Main(string[] args)
        {
            int n = 0;

            n = ExtractInt();

            Stopwatch sw = new Stopwatch();
            sw.Start();
            if (IsPrime(n))
            {
                Console.WriteLine("Numarul este prim");
            }
            else
	        {
                Console.WriteLine("Numarul nu este prim");
	        }
            sw.Stop();
            Console.WriteLine("Timpul total de executie a fost: {0}", sw.ElapsedMilliseconds);

        }

        private static bool IsPrime(int n)
        {
            if (n < 2)
            {
                return false;
            }

            if (n == 2)
            {
                return true;
            }

            int c = 0, d = 2;

            do
            {
                if (n % d == 0)
                {
                    c = c + 1; // c++
                }
                d = d + 1;
            } while ((c == 0) && (d * d <= n));

            if (c == 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }
    }
}