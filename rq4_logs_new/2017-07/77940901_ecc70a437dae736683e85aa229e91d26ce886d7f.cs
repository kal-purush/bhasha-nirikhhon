//Rextester.Program.Main is the entry point for your code. Don't change it.
//Compiler version 4.0.30319.17929 for Microsoft (R) .NET Framework 4.5

using System;
using System.Collections.Generic;
using System.Linq;

namespace TechWeekChallenge2
    {
    public class Program
        {
        public static void Main (string[] args)
            {
            List<int> data1 = new List<int> { 1, 2, 3, 4, 5 };
            List<string> data2 = new List<string> { "6", "3", "4", "" };

            var newData = data1.Select(i => i.ToString()).Intersect(data2);
            foreach ( var v in newData )
                {
                Console.WriteLine(v);
                }
            Console.ReadKey();
            }
        }
    }