using System;
using System.Numerics;
using System.Collections.Generic;

namespace task_DEV_4
{
    // Class describing the sequence of integers.
    public class IntegerNumberSequence
    {
        // Array specifying the sequence to be examined.
        private BigInteger[] numberSequence;
        // A sequence property that determines whether it is non-decreasing.
        private bool nonDecreasing;
        
        public IntegerNumberSequence()
        {
            numberSequence = new BigInteger[0];
            nonDecreasing = true;
        }

        public IntegerNumberSequence(int size)
        {
            numberSequence = new BigInteger[size];
            nonDecreasing = true;
        }

        // Set the value of the sequence.
        public BigInteger[] SequenceValues
        {
            set 
            {
                numberSequence = value;
            }
        }
        public bool IsNonDecreasing
        {
            get 
            { 
                return nonDecreasing;
            }
        }

        // A method to determine whether a sequence is non-decreasing.
        public bool CheckSequenceForNonDecreasing()
        {
            bool isNonDecreasing = true;
            BigInteger previousMember = numberSequence[0];
            foreach (BigInteger member in numberSequence)
            {
                if (member < previousMember)
                {
                    isNonDecreasing = false;
                    nonDecreasing = false;
                    break;
                }
                previousMember = member;
            }
            return isNonDecreasing;
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            // Input sequence of integers, conversion to BigInteger, add sequence to 
            // IntegerNumberSequence object.
            Console.WriteLine("Enter the sequence of integer numbers (separate numbers with spaces): \n");
            char delimiter = ' ';
            string[] inputSequenceString = Console.ReadLine().Split(delimiter);
            IntegerNumberSequence inputSequence = new IntegerNumberSequence(inputSequenceString.Length);

            try
            {
                BigInteger[] parsedSequence = new BigInteger[inputSequenceString.Length];
                for (int i = 0; i < inputSequenceString.Length; i++)
                {
                    parsedSequence[i] = BigInteger.Parse(inputSequenceString[i]);
                }
                inputSequence.SequenceValues = parsedSequence;
            }
            catch (FormatException)
            { 
                Console.WriteLine("Sorry, the entered sequence is incorrect! Exiting process.");
                Environment.Exit(-1);
            }

            // Check sequence for non-decreasing. Print answer to the console.
            string outputMessage = inputSequence.CheckSequenceForNonDecreasing()
                ? "The entered sequence is non-decreasing."
                : "The entered sequence isn't non-decreasing.";
            Console.WriteLine(outputMessage);
        }
    }
}