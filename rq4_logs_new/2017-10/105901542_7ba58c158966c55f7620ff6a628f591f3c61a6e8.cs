using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Zadanie2
{
    internal class Program
    {
        static void Iteracja()
        {
            double[] tab = new double[1000];
            tab[0] = 1;
            tab[1] = 1 / 2;
            tab[2] = 1.5;
            for (int i = 3; i < 1000; i++)
            {
                tab[i] = tab[i - 3] + (1 / 2) * tab[i - 2] - (5 / 4) * tab[i - 1];
            }
            Console.WriteLine(tab[999]);
        }
        public static void Main(string[] args)
        {
            var sw = new Stopwatch();
            sw.Start();
            Iteracja();
            Console.WriteLine(sw.Elapsed);

        }
    }
}