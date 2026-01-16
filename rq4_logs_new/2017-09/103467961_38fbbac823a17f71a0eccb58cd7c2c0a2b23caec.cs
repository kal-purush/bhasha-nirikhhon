using System;

namespace UI
{
    public static class Input
    {
        public static string RequestString()
        {
            string input;
            while (true)
            {
                input = Console.ReadLine();
                if (!string.IsNullOrWhiteSpace(input))
                    break;
                Console.WriteLine("Enter a non-empty string");
            }

            return input;
        }

        public static int SelectOption(string message, int min, int max)
        {
            Console.WriteLine(message);
            int option;
            while (true)
            {
                string inputOption = Console.ReadLine();
                bool didParse = int.TryParse(inputOption, out option);
                if (didParse)
                {
                    if (option >= min && option <= max)
                        break;
                    Console.WriteLine("Select a valid option");
                }
                else
                {
                    Console.WriteLine("Input must be a number");
                }
            }

            return option;
        }
    }
}