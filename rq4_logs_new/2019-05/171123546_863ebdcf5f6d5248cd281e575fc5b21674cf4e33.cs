using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Calculator
{
    static class BracketBalance
    {
        public static bool Check(string input)
        {
            Stack<char> stack = new Stack<char>();
            for (int i = 0; i < input.Length; ++i)
            {
                if (input[i] == '(')
                {
                    stack.Push(input[i]);
                }
                if (input[i] == ')')
                {
                    if (stack.Count == 0)
                    {
                        return false;
                    }
                    stack.Pop();
                }
            }
            return stack.Count == 0;
        }
    }
}