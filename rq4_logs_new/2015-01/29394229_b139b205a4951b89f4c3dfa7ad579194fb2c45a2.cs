using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Day9_2
{
    class Program
    {
        static void Main(string[] args)
        {

            var first = 7; //infers type int
            Console.WriteLine("First var type is {0}" , first.GetType());

            var second = "This is my string";
            Console.WriteLine("First var type is {0}", second.GetType());

            var third = second; 
            Console.WriteLine("First var type is {0}", third.GetType());

            MyHelper();

            string[] states = { "Ca", "mn", "Tx", "CO" };

            var istates = from s in states
                            where s.StartsWith("C")
                            select s;
            foreach (var state in istates)
            {
                Console.WriteLine(state);
            }

            Console.WriteLine("");
            Console.WriteLine("Press any key to continue...");
            Console.ReadLine();
        }

        static void MyHelper()
        {
            var sixth = 2;
        }
    }
}