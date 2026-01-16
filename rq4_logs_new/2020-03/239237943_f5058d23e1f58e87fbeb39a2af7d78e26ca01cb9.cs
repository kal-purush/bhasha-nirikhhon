using System;
using System.Linq;
using System.Net;

namespace Calculator
{
    public class NegativesNotAllowedException : Exception
    {
        public NegativesNotAllowedException(CalculatorInput calculatorInput) : base(FormatMessage(calculatorInput))
        {
        }

        private static string FormatMessage(CalculatorInput calculatorInput)
        {
            var numbers = calculatorInput.StringNumbers.Split(calculatorInput.Separators, StringSplitOptions.None);
            var negativeNumbers = numbers.Where(num => Convert.ToInt32(num) < 0).ToArray();
            return "Negatives not allowed: " + string.Join(", ", negativeNumbers);
        }
    }
}