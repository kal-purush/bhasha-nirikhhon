using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace FizzBuzz
{
    class Program
    {
        static void Main(string[] args)
        {
            Enumerable.Range(1, 100).ToList().ForEach(n =>
            {
                var stringBuilder = new StringBuilder();
                if (n % 3 == 0)
                {
                    stringBuilder.Append("Fiku");
                }
                if (n % 5 == 0)
                {
                    stringBuilder.Append("Miku");
                }

                if (stringBuilder.Length == 0)
                {
                    stringBuilder.Append(n);
                }

                Console.WriteLine(stringBuilder);
            });
        }
    }
}