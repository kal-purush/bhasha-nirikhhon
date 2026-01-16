using System;

namespace Challenge;
class Program
{
    static void Main(string[] args)
    {
        // SmallExercise();

        string input = "0";
        int count = 0;
        int total = 0;
        int currentNumber = 0;

        while (input != "-1")
        {
            Console.WriteLine("Last number was {0}", currentNumber);
            Console.WriteLine("Please enter the next score");
            Console.WriteLine("Current amount of entries {0}", count);
            Console.WriteLine("Please enter -1 once you are ready to calculate the average");

            input = Console.ReadLine();
            if (input.Equals("-1"))
            {
                Console.WriteLine("--------------------------------------------");
                double average = (double)total / (double)count;
                Console.WriteLine("The average score of your students is {0}", average);
            }
            if (int.TryParse(input, out currentNumber) && currentNumber > 0 && currentNumber < 21)
            {
                total += currentNumber;
            }
            else
            {
                if (!(input.Equals("-1")))
                {
                    Console.WriteLine("Please enter a value between 1 and 20!");
                }
                continue;
            }

            count++;
        }
    }

    static void SmallExercise()
    {
        int i = -10;

        while (true)
        {
            if (i % 3 == 0)
            {
                i++;
                continue;
            }
            if (i == 10)
            {
                break;
            }
            if (i == 11)
            {
                Console.WriteLine(" FINAL BREAK REACHED! This should not happen!");
                break;
            }
            Console.WriteLine(i++);
        }
    }
}