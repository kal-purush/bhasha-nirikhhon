using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mathmagician
{
    public class Odd
    {
        // Stores the resulting list of odd numbers
        List<int> oddNumbers = new List<int>();
        
        // Counts all integers up to the user-entered count value and adds only the values 
        //  that have a non-zero modulus value
        public void CountOddNumbers(int SentNumOfOdds)
        {
            for (int count = 1; oddNumbers.Count < SentNumOfOdds; count++)
            {
                if (count % 2 != 0)
                {
                    oddNumbers.Add(count);
                }
            }
        }

        // Returns the list of odd numbers for displaying
        public List<int> OddList()
        {
            return oddNumbers;
        }
    }
}