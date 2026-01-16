using System;
using System.Linq;

/* Sample input 0:
 * 5 3
 * 4 2 6 1 10
 * Sample output 0:
 * 4
 */
namespace Implementations
    {
    /// <summary>
    /// Lisa's Workbook
    /// https://www.hackerrank.com/challenges/lisa-workbook
    /// </summary>
    public class LisaWorkbook
        {
        public static void Main (string[] args)
            {
            string[] input1 = Console.ReadLine().Split(' ');
            string[] input2 = Console.ReadLine().Split(' ');

            int chapters = Convert.ToInt32(input1[0]);
            int limit = Convert.ToInt32(input1[1]);

            int[] problems = Array.ConvertAll(input2, Int32.Parse);
            int resultantCount = 0;
            int page = 1;

            for ( int i = 0; i < chapters; i++ )
                {
                int pages = Convert.ToInt32(Math.Ceiling(problems[i] / Convert.ToDecimal(limit)));
                int maxProbCount = pages * limit;
                for ( int j = 1; j <= maxProbCount; j += limit )
                    {
                    if ( problems[i] >= j && problems[i] <= (j + limit - 1) )
                        {
                        while ( j <= problems[i] )
                            {
                            if ( j == page )
                                resultantCount++;
                            j++;
                            }
                        }
                    else
                        {
                        if ( Enumerable.Range(j, limit).Contains(page) )
                            resultantCount++;
                        }
                    page++;
                    }
                }
            Console.WriteLine(resultantCount);
            Console.ReadKey();
            }
        }
    }