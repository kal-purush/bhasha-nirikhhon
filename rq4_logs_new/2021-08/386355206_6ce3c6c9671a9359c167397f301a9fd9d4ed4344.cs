using System.Linq;

namespace RandomKatas
{
    /*  Given an array of integers.
        Return an array, where the first element is the count of positives numbers and the second element
        is sum of negative numbers.
        If the input array is empty or null, return an empty array.
        Example
        For input [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, -11, -12, -13, -14, -15], you should return [10, -65]. */
    
    public class CountOfPositivesSumOfNegatives
    {
        public static int[] countPositivesSumNegatives(int[] input)
        {
            if (input == null || input.Length == 0) return new int[0]; 
            int count = 0;
            int sum = 0;
            input.Select(n => n > 0 ? count++ : sum += n).ToList();
            
            return new[] {count, sum};
            
            // return (input == null || input.Length ==0) ? new int[0] : new int[] { input.Count(o => o > 0),
            //     input.Where(o => o < 0).Sum() };
        }
    }
}