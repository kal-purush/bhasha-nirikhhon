using System;
using StackAndQueue;

namespace MultiBracketValidation
{
    public class Program
    {
        static void Main(string[] args)
        {
        }

        public static bool MultiBracketValidation(string input)
        {
            MyStack<char> bracketOpeners = new MyStack<char>();

            foreach (char currentCharacter in input)
            {
                switch (currentCharacter)
                {
                    // Intentional fall-through. Push any opening bracket to the
                    // stack to be checked when encountering a closing bracket
                    case '(':
                    case '[':
                    case '{':
                        bracketOpeners.Push(currentCharacter);
                        break;
                    // Check for corresponding opener if encountering a closer.
                    // Pop off of the stack so we can check after the loop for an
                    // unclosed bracket
                    case ')':
                        if (bracketOpeners.Pop() != '(')
                        {
                            return false;
                        }
                        break;
                    case ']':
                        if (bracketOpeners.Pop() != '[')
                        {
                            return false;
                        }
                        break;
                    case '}':
                        if (bracketOpeners.Pop() != '{')
                        {
                            return false;
                        }
                        break;
                }
            }

            // If there are any openers left on the stack, then the input
            // did not close all of its brackets
            return bracketOpeners.Length < 1;
        }
    }
}