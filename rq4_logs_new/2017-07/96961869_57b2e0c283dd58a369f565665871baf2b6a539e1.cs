using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MatchString
{
    class Program
    {
        static string unit = "ymwdh";
        static void Main(string[] args)
        {
            Console.WriteLine("Input:");
            var input = Console.ReadLine();

            Console.WriteLine(match(input));
            Console.ReadKey();
        }
        static string match(string s)
        {
            string head = s.Substring(0, s.Length - 1);
            string tail = s.Substring(s.Length - 1);

            int tem = 0;
            if (!int.TryParse(head, out tem))
            {
                return "error";
            }

            else
            {
                if (Char.IsDigit(tail, 0))
                    return s + "y";

                if (Char.IsLetter(tail, 0))
                {

                    if (!unit.Contains(tail))
                        return "error";
                    else return s;
                }
            }
            return s;
        }
    }
}