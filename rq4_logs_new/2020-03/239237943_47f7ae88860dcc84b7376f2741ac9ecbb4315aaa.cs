using System;

namespace Calculator
{
    public class StringCalculator
    {
        public int Add(string stringNumbers )
        {
            var numbers = stringNumbers.Split(new string[] {","}, StringSplitOptions.None);

            var totalSum = 0;
            
            for (int i = 0; i < numbers.Length; i++)
            {
                totalSum += Convert.ToInt32(numbers[i]);
            }

            return totalSum;

            //return Convert.ToInt32(stringNumber);
            //return 0;
        }
    }
}