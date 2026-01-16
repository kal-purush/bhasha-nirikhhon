using System;
using System.Collections.Generic;
using System.Linq;

namespace Exercise5
{
    class Program
    {
        static void Main(string[] args)
        {
            var schedule = new Dictionary<string, string>()
            {
                {"English III", "Ms. Lapan"},
                {"Precalculus", "Mrs. Gideon"},
                {"Music Theory", "Mr. Davis"},
                {"Biotechnology", "Ms. Palmer"},
                {"Principles of Technology I", "Ms. Garcia"},
                {"Latin II", "Mrs. Barnett"},
                {"AP US History", "Ms. Johannessen"},
                {"Business Computer Infomation Systems", "Mr. James"}
            };

            var border = $"+{string.Concat(Enumerable.Repeat("-", 54))}+";

            Console.WriteLine(border);
    
            for (var i = 0; i < schedule.Count; i++)
            {
                Console.WriteLine("|{0,1}|{1,36}|{2,15}|", i + 1, schedule.ElementAt(i).Key, schedule.ElementAt(i).Value);
            }

            Console.WriteLine(border);
        }
    }
}