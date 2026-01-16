using System;
using System.Collections.Generic;
using System.Linq;

namespace Exercise6
{
    class Program
    {
        static void Main(string[] args)
        {
            string res;
            Console.WriteLine("Enter sequence of numbers:");
            do
            {
                int sum = 0;
                string enteredNums = Console.ReadLine();
                char[] charArr = enteredNums.ToCharArray();

                var numberArray = charArr.Select(x =>
                    {
                        int value;
                        bool success = int.TryParse(x.ToString(), out value);
                        return new {value, success};
                    })
                    .Where(pair => pair.success)
                    .Select(pair => pair.value)
                    .ToList();

                foreach (var number in numberArray)
                {
                    sum += number;
                }

                Console.WriteLine(sum);
                Console.WriteLine("Press - y - to continue");

                res = Console.ReadLine();

            } while (res == "y");
        }
    }
}