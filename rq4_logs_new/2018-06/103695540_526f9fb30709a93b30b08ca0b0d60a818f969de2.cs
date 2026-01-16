/* Ex. 9.4 (Duplicate Word Removal) Write a console app that inputs a sentence from the user,
 * then determines and displays the nonduplicate words in alphabetical oder.
 * Treat upper and lower case letters the same.
 */
 //VeryAble 11-9-2017

using System;
using System.Linq;

namespace wordRemoval
{
	class wordRemoval
	{
		static void Main(string[] args)
		{
			Console.WriteLine("Please enter a sentence: \n"); // Prompt user
			string response = Console.ReadLine(); // Place input from user into a string variable

			var sentence = response.ToLower();

			string[] split = sentence.Split(); // Place each word from the string variable into an array of strings

			var words = split.Distinct();

			Console.WriteLine("\nFirst we look at each word and compare for duplicates...\n");
			foreach (var word in split)
			{
				Console.Write($"{word}, ");
			}

			Console.WriteLine("\n"); // Space

			Console.WriteLine("and here is your sentence without any duplicate words:\n");

			foreach ( var word in words)
			{
				Console.Write($"{word} ");
			}

			Console.WriteLine("\n"); // Space

			Console.WriteLine("Now in alphabetical order:\n");

			var alphaSorted =
				from alpha in words
				orderby alpha
				select alpha;

			foreach (var word in alphaSorted)
			{
				Console.Write($"{word} ");
			}

			Console.WriteLine("\n"); // Space
		}
	}
// Thanks for looking
}