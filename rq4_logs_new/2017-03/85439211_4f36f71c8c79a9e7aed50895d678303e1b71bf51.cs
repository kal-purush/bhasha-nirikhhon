using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Calculator
{
    class CalculatorBase
    {
        private char[] Allowed { get { return new char[] { '(', ')', ',', '.', '+', '-', '*', '/', ' ' }; } }
        public string TrimInput(string input)
        {
            return new string(input.ToCharArray().Where(c => !Char.IsWhiteSpace(c)).ToArray());
        }
        private bool ValidateInput(string input)
        {
            bool error = false;
            int open = 0;
            int close = 0;
            foreach (char symbol in input)
            {
                if (!(Char.IsDigit(symbol) || this.Allowed.Contains(symbol)))
                    error = true;
                if (symbol == '(')
                    open++;
                if (symbol == ')')
                    close++;
            }
            return ((open == close) && (!error));
        }
    }
}