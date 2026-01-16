using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Implementations
    {
    public class ChocolateFeast
        {
        public static void Main (string[] args)
            {
            int trips = Convert.ToInt32(Console.ReadLine());
            List<int> results = new List<int>(trips);
            for ( int i = 0; i < trips; i++ )
                {
                string input = Console.ReadLine();
                int result = GetTotalNumberOfChocolate(input);
                results.Add(result);
                }
            foreach ( int result in results )
                {
                Console.WriteLine(result);
                }
            Console.ReadKey();
            }

        private static int GetTotalNumberOfChocolate (string input)
            {
            string[] inputs = input.Split(' ');
            int dollers = Convert.ToInt32(inputs[0]);
            int rateOfChocolate = Convert.ToInt32(inputs[1]);
            int tradeRate = Convert.ToInt32(inputs[2]);
            bool flag = true;
            int chocolateCount = 0;
            int remainingWrappers = 0;
            do
                {
                if ( chocolateCount == 0 || chocolateCount >= tradeRate )
                    {
                    chocolateCount += dollers / rateOfChocolate;
                    //remainingWrappers = chocolateCount % tradeRate;
                    dollers = chocolateCount;
                    rateOfChocolate = tradeRate;
                    }
                else
                    flag = false;
                } while ( flag );
            return chocolateCount;
            }
        }
    }