using System;
using System.Collections.Generic;
using System.Diagnostics;
using static System.Console;

namespace Homework_1
{
    class Program
    {
        const int SIZE = 10_000_000;
        const int ITERATIONS = 10;
        static string[] strings = new string[SIZE];
        static HashSet<string> hashSet = new HashSet<string>(SIZE);
        
        static void Main(string[] args)
        {
            Fill();

            for (int i = 1; i <= ITERATIONS; i++)
            {
                Random rnd = new Random();
                string search = strings[rnd.Next(0, SIZE - 1)];
                WriteLine($"Iteration N{i}:");
                TestFindArray(search);
                TestFindHashSet(search);
                WriteLine();
            }
            ReadKey();
        }

        static void Fill()
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            for (int i = 0; i < SIZE; i++)
            {
                string text = Guid.NewGuid().ToString();
                strings[i] = text;
                hashSet.Add(text);
            }
            sw.Stop();
            WriteLine($"Fill time: {sw.ElapsedTicks / 10000.0}мс");
        }

        static void TestFindArray(string search)
        {
            int result;
            Stopwatch sw = new Stopwatch();
            sw.Start();
            result = Array.IndexOf(strings, search);
            sw.Stop();
            if (result >=0)
                WriteLine($"Array search time: {sw.ElapsedTicks/10000.0}мс");
            else
                WriteLine($"Not found in array!");
        }

        static void TestFindHashSet(string search)
        {
            bool result;
            Stopwatch sw = new Stopwatch();
            sw.Start();
            result = hashSet.Contains(search);
            sw.Stop();
            if (result)
                WriteLine($"HashSet search time: {sw.ElapsedTicks/10000.0}мс");
            else
                WriteLine($"Not found in HashSet !");
        }
    }
}