using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace CountVowels
{
    class Program
    {
        static void Main(string[] args)
        {
            string enterString = Console.ReadLine();
            var vowels = new List<char> { 'a', 'e', 'i', 'o', 'u' };
            int y = 0;
            foreach(char x in enterString.ToCharArray())
            {
                if (vowels.Contains(x))
                    y++;
            }
            Console.WriteLine(y);
            
            Console.ReadLine();
        }
    }
}