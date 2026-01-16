using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Exercise4
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Enter number: ");
            string s = Console.ReadLine();
            int counter = 0;
            var unique = new List<string>();
            while (s!="1" && counter < 100 && !unique.Contains(s))
            {
                int sum = 0;
                unique.Add(s);
                for (int i = 0; i < s.Length; i++)
                {
                    sum += int.Parse(s[i].ToString()) * int.Parse(s[i].ToString());
                }
                
                counter++;
                s = sum.ToString();
                Console.Write(s + ",");
            }

            if (s == "1")
            {
                Console.WriteLine("Happy one!!!:):)");
            }
            else
            {
                Console.WriteLine("Not happy.... :(");
            }

            Console.ReadKey();
        }
    }
}