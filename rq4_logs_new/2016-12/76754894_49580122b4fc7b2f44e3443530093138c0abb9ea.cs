using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace _1000DaysAfterBirth
{
    class Program
    {
        private static object Culture;

        static void Main(string[] args)
        {
            string input = Console.ReadLine();

            DateTime dt = DateTime.ParseExact(input, "dd-MM-yyyy", CultureInfo.InvariantCulture);
            DateTime result = dt.AddDays(1000 - 1);

            Console.WriteLine(result.ToString("dd-MM-yyyy", CultureInfo.InvariantCulture));
        }
    }
}