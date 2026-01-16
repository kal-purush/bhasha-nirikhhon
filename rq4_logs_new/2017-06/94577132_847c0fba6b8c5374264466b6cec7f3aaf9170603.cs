using System;
using System.Numerics;

namespace task_DEV_4
{
    // A program that determines whether the entered into the console 
    // sequence of integers is nondecreasing.
    class EntryPoint
    {
        static void Main(string[] args)
        {
            NumberConsoleInputHelper inputHelper = new NumberConsoleInputHelper();
            BigInteger[] enteredSequence = inputHelper.GetInputNumberSequence();
            IntegerNumberSequence examinedSequence = new IntegerNumberSequence(enteredSequence);

            // Check sequence for non-decreasing. Print answer to the console.
            string outputMessage = examinedSequence.CheckSequenceForNonDecreasing()
                ? "The entered sequence is non-decreasing."
                : "The entered sequence isn't non-decreasing.";
            Console.WriteLine(outputMessage);
        }
    }
}