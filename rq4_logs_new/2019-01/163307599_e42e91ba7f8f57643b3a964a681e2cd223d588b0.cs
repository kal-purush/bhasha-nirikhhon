using System;
using System.Collections.Generic;
using System.Linq;

namespace BasicFuncs.App
{
    public class Program
    {
        /// <summary>
        /// Repeat entered input.
        /// </summary>
        public static void RepeatInput()
        {
            string input = Console.ReadLine();
            Console.WriteLine("The string entered:");
            Console.WriteLine(input);
        }
        /// <summary>
        /// Concatenate three strings entered by a user.
        /// </summary>
        public static void ConcatInput()
        {
            Console.WriteLine("Enter three lines of text");
            List<string> input = new List<string>();
            for (int i = 0; i < 3; i++)
            {
                input.Add(Console.ReadLine());
            }
            Console.WriteLine("You entered:");
            Console.WriteLine(string.Join(";", input));
        }
        /// <summary>
        /// Change array element at position
        /// </summary>
        /// <param name="arr">Array of integers</param>
        /// <param name="position">Zero-based position of an element to change</param>
        /// <param name="value">New value of an element</param>
        public static void ChangeArrayValueAtPosition(int[] arr, int position, int value)
        {
            if (position > arr.Length)
                return;
            arr[position] = value;
        }
        /// <summary>
        /// Filter words at odd positions from a string
        /// </summary>
        /// <param name="inputString">A string to filter</param>
        /// <returns>Filtered string</returns>
        public static string FilterOdd(string inputString)
        {
            string[] words = inputString.Split(' ');
            if (words.Length == 1)
                return inputString;
            var result = words.Where((elm, ndx) => ndx % 2 == 0);
            return string.Join(" ", result.ToArray());
        }
        public static string SubString(string inputString, int startFrom = 0, int length = 0)
        {
            string[] words = inputString.Split(' ');
            if (words.Length < length ||
                words.Length < startFrom ||
                words.Length < startFrom + length)
                return string.Empty;
            var result = words.Where((elm, ndx) => ndx >= startFrom && ndx <= startFrom + (length  == 0 ? words.Length : length));
            return string.Join(" ", result);
        }
        public static void Main(string[] args)
        {
            Console.WriteLine("1. Repeat the string. Type-in a string and press Enter.");
            RepeatInput();

            Console.WriteLine("2. Concatenate strings.");
            ConcatInput();

            Console.WriteLine("3. Replace value in array at position.");
            int[] arr = new int[] { 1,2,3,4,5 };
            int position = 2;
            int newValue = 100;
            Console.WriteLine($"   Initial array: {string.Join(", ", arr)}, position to change: {position}, new value: {newValue}");
            ChangeArrayValueAtPosition(arr, position, newValue);
            Console.WriteLine($"   Mutated array: {string.Join(", ", arr)}");

            Console.WriteLine("4. Filter words at odd positions from a string.");
            string inpString = "to be or not to be";
            string outString = FilterOdd(inpString);
            Console.WriteLine($"   Initial string: '{inpString}', result: '{outString}'");

            Console.WriteLine("5. Get substring from a string.");
            outString = SubString(inpString, 3);
            Console.WriteLine($"   Initial string: '{inpString}', result: '{outString}'");
        }
    }
}