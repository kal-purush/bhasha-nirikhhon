using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Exercise31
{
    class Program
    {
        static void Main(string[] args)
        {
            string s = "Alice";
            var names = new List<string>();
            while (s != "")
            {
                Console.WriteLine("Enter name: ");
                s = Console.ReadLine();
                if (s != "" && !names.Contains(s))
                {
                    names.Add(s);
                }
            }
            
            Console.Write("Unique name list contains: ");
            foreach (var i in names)
            {
                Console.Write(i+" ");
            }

            Console.ReadKey();
        }
    }
}