using System;
using System.Linq;

namespace ConsoleApp1
{
    class Program
    {
        /*............................. Drawing Book ...............................

        LINK :https://www.hackerrank.com/challenges/drawing-book/problem
        */

        static void Main(string[] args)
        {
            //Test test = new Test();

            //test.conga();

            int pageCount(int n, int p)
            {
                if (p == 1 || p == n) return 0;

                int flipTimes = -1;
                int flipTimesBack = -1;


                for (int i = 1; i < n; i += 2)
                {
                    flipTimes += 1;
                    int[] page = { i - 1, i };
                    if (page.Contains(p)) break;
                }


                if (n % 2 == 0) n += 1;

                for (int i = n; i > 1; i -= 2)
                {
                    flipTimesBack += 1;
                    int[] page = { i - 1, i };
                    if (page.Contains(p)) break;
                }

                return flipTimes > flipTimesBack ? flipTimesBack : flipTimes;

            }



            int n = Convert.ToInt32(Console.ReadLine().Trim());

            int p = Convert.ToInt32(Console.ReadLine().Trim());

            int result = pageCount(n, p);

            Console.WriteLine(result);


            Console.ReadKey();
        }
    }
}