using System;
using System.Linq;
using System.Text.RegularExpressions;
using static System.Console;

namespace WordClassLibrary
{
    public class ConsoleReader
    {
        public void Run()
        {
            while (true)
            {
                WriteLine("Please enter word for console read , for stop please enter \'exit\'");
                var input = ReadLine();

                if (string.IsNullOrEmpty(input)) {
                    Clear();
                    WriteLine("Not Word Passed Please try Again ! ");
                    continue;
                }

                if ("exit".Equals(input, StringComparison.OrdinalIgnoreCase)) break;

                InputParser(input);

                Read();
                Clear();
            }
        }

        private static void InputParser(string input)
        {
            var r = new Regex("^[a-zA-Z0-9]*$");
            var handle = new Event();

            if (input.All(char.IsDigit))
            {
                EventDelegates.OnNumber delegateRef = handle.EventHandler;
                delegateRef.Invoke("OnNumber called - Given string is numeric");
            }
            else if (r.IsMatch(input))
            {
                EventDelegates.OnWord delegateRef = handle.EventHandler;
                delegateRef.Invoke("OnWord called - Given string is alphanumeric");
            }
            else
            {
                EventDelegates.OnJunk delegateRef = handle.EventHandler;
                delegateRef.Invoke("OnJunk called - Given string is other");
            }
        }
    }
}