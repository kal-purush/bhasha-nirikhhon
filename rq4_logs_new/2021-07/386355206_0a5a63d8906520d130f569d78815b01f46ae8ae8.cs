using System;
using static Recursion.Tasks.AckermannFunction;
using static Recursion.Tasks.Reverse;
using static Recursion.Tasks.SecOrRev;

namespace Recursion
{
    class Program
    {
        static void Main(string[] args)
        {
            // A: Дано натуральное число n. Выведите все числа от 1 до n. (DONE)
            
            // ReverseNumber(5);
            
            // B: Даны два целых числа A и В (каждое в отдельной строке). Выведите все числа от A до B включительно,
            //    в порядке возрастания, если A < B, или в порядке убывания в противном случае. (UNDONE)
            
            // SequenceOrReverse(5, 1);
            // SequenceOrReverse(1, 5);
            
            // C: Даны два целых неотрицательных числа m и n, каждое в отдельной строке. Выведите A(m,n). (DONE)

            Console.WriteLine(ComputableFunction(2, 2));
            


        }
    }
}