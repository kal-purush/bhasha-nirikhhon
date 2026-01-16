// See https://aka.ms/new-console-template for more information

Console.WriteLine("==== Reverse Vowels using Char Array Test ====");

var stringForTest = "hello";
Console.WriteLine($"original: {stringForTest}");
Console.WriteLine($"output: {ReverseVowels.ReverseVowels.ReverseUsingArray(stringForTest)}");

stringForTest = "AEIOU";
Console.WriteLine($"original: {stringForTest}");
Console.WriteLine($"output: {ReverseVowels.ReverseVowels.ReverseUsingArray(stringForTest)}");

stringForTest = "DesignGUrus";
Console.WriteLine($"original: {stringForTest}");
Console.WriteLine($"output: {ReverseVowels.ReverseVowels.ReverseUsingArray(stringForTest)}");

Console.ReadKey();