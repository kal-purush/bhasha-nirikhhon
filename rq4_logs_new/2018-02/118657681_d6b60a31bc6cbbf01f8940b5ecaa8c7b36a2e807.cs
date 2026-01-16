using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Iterators
{
    public class Example1
    {
        public static void Mains()
        {
            var sequence = GetSequence();

            // EXAMPLE 1: We need to evaluate the values in order to iterate over the collection 

            foreach (var elem in sequence)
                Console.WriteLine(elem);

            // EXAMPLE 2: We need to evaluate values to calculate the sum

            sequence.Sum();

            // EXAMPLE 3: We need to evaluate values to put them into List<int>

            var materializedSequence = sequence.ToList();


            Console.ReadLine();
        }

        public static IEnumerable<int> GetSequence()
        {
            while (true) yield return 1;
        }
    }
}