using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace CameraView
{
    class Program
    {
        static void Main(string[] args)
        {
            List<string> result = new List<string>();
            int[] numbers = Console.ReadLine().Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries).Select(int.Parse).ToArray();
            string input = Console.ReadLine();
            string pattern = @"(?:\|\<)(?:.{" + numbers[0] + @"})([\w]{0," + numbers[1] + "})";

            var match = Regex.Matches(input, pattern);
            foreach (Match m in match)
            {
                result.Add(m.Groups[1].ToString());
            }
            var resultt = String.Join(", ", result.ToArray());

            Console.WriteLine(resultt);
        }
    }
}