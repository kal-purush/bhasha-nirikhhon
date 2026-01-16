using System;
using System.Collections.Generic;

namespace whiteboard27th
{
    class Program
    {
        static void Main(string[] args)
        {
            IEnumerable<string> result = Filter(new List<string> { "red", "to", "blue", "ford", "no" }, 2);
            // foreach (var item in result)
            // {
            //     Console.WriteLine(item);
            // }
            IEnumerable<string> another = AnotherType();
            foreach (var item2 in another)
            {
                Console.WriteLine(item2);
            }
        }

        private static IEnumerable<string> AnotherType()
        {
            IEnumerable<string> typeArray = Filter(new string[] { "red", "to", "blue", "ford", "no" }, 2);
            typeArray = Filter(typeArray, 3);
            return typeArray;
        }
        private static IEnumerable<string> Filter(IEnumerable<string> list, int howMany)
        {
            foreach (var item in list)
            {
                if (item.Length > howMany)
                {
                    yield return item;
                }
            }
        }
    }
}