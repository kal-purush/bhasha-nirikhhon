using System;

namespace Dev1
{
    /// <summary>
    /// Class EntryPoint which realized SearchMethod.
    /// Finds sequences in string without repetitive in a row elements.
    /// </summary>
    class EntryPoint
    {
        /// <summary>
        /// Entry point.
        /// Checks input string and calls checker's methods.
        /// </summary>
        /// <param name="args">The command line arguments</param>
        static void Main(string[] args)
        {
            try
            {
                if (args.Length < 2 && args[0].Length < 2)
                {
                    throw new FormatException();
                }
                else
                {
                    var sequencesFinder = new SequencesSearcher(args);
                    sequencesFinder.DisplaySequence(sequencesFinder.FindSequences());
                }
            }
            catch (FormatException)
            {
                Console.WriteLine("Error. String contains less than 2 elements.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An exception: {ex.Message}");
            }
        }
    }
}