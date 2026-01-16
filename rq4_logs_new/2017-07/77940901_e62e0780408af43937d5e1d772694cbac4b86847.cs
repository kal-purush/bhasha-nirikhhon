using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Stack
    {
    public class BalancedBrackets
        {
        public static void Main (string[] args)
            {
            List<string> results = new List<string>();

            int inputs = Convert.ToInt32(Console.ReadLine());
            for ( int i = 0; i < inputs; i++ )
                {
                Stack<char> bracket = new Stack<char>();
                string input = Console.ReadLine();
                bool result = IsBalanced(bracket, input);
                results.Add(result ? "YES" : "NO");
                }
            foreach ( var res in results )
                {
                Console.WriteLine(res);
                }
            Console.ReadKey();
            }

        private static bool IsBalanced (Stack<char> bracket, string input)
            {
            foreach ( var bracketChar in input )
                {
                if ( bracketChar == '(' || bracketChar == '[' || bracketChar == '{' )
                    bracket.Push(bracketChar);
                else
                    {
                    if ( bracket.Count != 0 )
                        {
                        char ch = bracket.Pop();
                        if ( ch == '(' && bracketChar != ')' )
                            return false;
                        if ( ch == '[' && bracketChar != ']' )
                            return false;
                        if ( ch == '{' && bracketChar != '}' )
                            return false;
                        }
                    else
                        return false;
                    }
                }
            if (bracket.Count != 0)
                return false;

            return true;
            }
        }
    }