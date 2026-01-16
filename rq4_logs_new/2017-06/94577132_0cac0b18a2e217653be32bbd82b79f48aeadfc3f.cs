using System;
using System.Linq;

namespace task_DEV_3
{
    // A program that determines whether the introduced nonnegative integer 
    // is a member of the Fibonacci sequence. 
    // Entered number size is limited to 254.
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Enter a non-negative integer number.");
            string inputValue = Console.ReadLine();

            // Representation of the entered string value of the number 
            // as an array of integers of size inputNumberPartSize each.
            // Check the format of the entered number.
            int inputNumberPartSize = int.MaxValue.ToString().Length - 1;
            int inputNumberPartsAmount = inputValue.Length /
                (inputNumberPartSize) + 1;
            int[] inputNumberAsArray = new int[inputNumberPartsAmount];

            try
            {
                for (int i = 0; i < inputNumberPartsAmount - 1; i++)
                {
                    inputNumberAsArray[i] = Convert.ToInt32(inputValue.
                        Substring((inputNumberPartsAmount - 2 - i) * (inputNumberPartSize) +
                        inputValue.Length % (inputNumberPartSize),
                        inputNumberPartSize));
                }
                inputNumberAsArray[inputNumberPartsAmount - 1] = Convert.ToInt32(inputValue.
                    Substring(0, inputValue.Length % (inputNumberPartSize)));
            }
            catch (FormatException)
            {
                Console.WriteLine("Sorry, the entered number is incorrect! Exiting process.");
                Environment.Exit(-1);
            }

            // Iterative calculation of the members of the Fibonacci row
            // and comparison with the introduced.
            // Two previously calculated numbers are stored in previousRowMembers.

            int[,] previousRowMembers = new int[2, inputNumberPartsAmount];
            previousRowMembers[0, 0] = 0;
            previousRowMembers[1, 0] = 1;
            int[] currentRowMember = new int[inputNumberPartsAmount];
            currentRowMember[0] = 1;

            bool completeCalculation = false;
            while (true)
            {
                // Comparison of the entered number and 
                // previously calculated Fibonacci row's member. 
                // If the entered number is less, then we finish the calculation.
                for (int i = inputNumberPartsAmount - 1; i >= 0; i--)
                {
                    if (inputNumberAsArray[i] != currentRowMember[i])
                    {
                        if (inputNumberAsArray[i] < currentRowMember[i])
                        {
                            completeCalculation = true;
                            break;
                        }
                        if (inputNumberAsArray[i] > currentRowMember[i])
                        {
                            break;
                        }
                    }
                }

                if (completeCalculation)
                {
                    break;
                }

                // Finding the next member of the Fibonacci series 
                // by adding the two previous ones.
                int previousDigitNumber = 0;
                for (int i = 0; i < inputNumberPartsAmount; i++)
                {
                    if ((previousRowMembers[0, i] + previousRowMembers[1, i] + previousDigitNumber).
                       ToString().Length > inputNumberPartSize)
                    {
                        currentRowMember[i] = (previousRowMembers[0, i] + previousRowMembers[1, i] +
                            previousDigitNumber) % Convert.ToInt32(Math.Pow(10, inputNumberPartSize));
                        previousDigitNumber = 1;
                    }
                    else
                    {
                        currentRowMember[i] = previousRowMembers[0, i] + previousRowMembers[1, i] +
                            previousDigitNumber;
                        previousDigitNumber = 0;
                    }

                    previousRowMembers[0, i] = previousRowMembers[1, i];
                    previousRowMembers[1, i] = currentRowMember[i];
                }
            }

            // Get the penultimate calculated member of the Fibonacci row.
            for (int i = 0; i < inputNumberPartsAmount; i++)
            {
                currentRowMember[i] = previousRowMembers[0, i];
            }

            // Comparison of the entered number with the penultimate 
            // calculated member of the Fibonacci row.
            // If they coincide then the entered number is a member of the row.
            // Output result to the console.
            string outputMessage = currentRowMember.SequenceEqual(inputNumberAsArray)
                ? "The entered number is a member of the Fibonacci sequence."
                : "The entered number isn't a member of the Fibonacci sequence.";

            Console.WriteLine(outputMessage);
        }
    }
}