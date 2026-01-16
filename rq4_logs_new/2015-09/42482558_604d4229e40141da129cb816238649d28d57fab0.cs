using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Date
{
    class Program
    {
        static void Main(string[] args)
        {
            DateTime currentTime = DateTime.Now;
            Console.WriteLine(currentTime.ToShortDateString());
            Console.WriteLine(currentTime.ToLongDateString());
            Console.WriteLine(currentTime.ToShortTimeString());
            Console.WriteLine(currentTime.ToLongTimeString());
            Console.ReadLine();
        }
    }
}