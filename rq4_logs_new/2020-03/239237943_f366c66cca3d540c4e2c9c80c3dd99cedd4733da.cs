using System;
using System.Linq;
using System.Text.RegularExpressions;

namespace Calculator
{
    public class DelimiterCannotHaveNumberOnTheEdgeException : Exception
    {
        public DelimiterCannotHaveNumberOnTheEdgeException(string invalidDelimiter) : base(FormatMessage(invalidDelimiter))
        {
        }

        private static string FormatMessage(string invalidDelimiter)
        {
            
            return "Delimiter cannot have a number on the edge: " + invalidDelimiter;
        }
    }
    
      
}