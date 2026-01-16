var words = new [] { "the", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog" };
var word1 = "fox";
var word2 = "dog";

Console.WriteLine(string.Join(',', words));
Console.WriteLine($"distance between {word1} and {word2} is: {WordDistance.WordDistance.FindShortestDistanceForWords(words, word1, word2)}");

words = new [] { "the", "quick", "brown", "fox", "fox", "over", "fox", "lazy", "dog" };
word1 = "fox";
word2 = "dog";

Console.WriteLine(string.Join(',', words));
Console.WriteLine($"distance between {word1} and {word2} is: {WordDistance.WordDistance.FindShortestDistanceForWords(words, word1, word2)}");

words = new [] { "the", "quick", "dog", "fox", "fox", "over", "fox", "lazy", "dog" };
word1 = "fox";
word2 = "dog";

Console.WriteLine(string.Join(',', words));
Console.WriteLine($"distance between {word1} and {word2} is: {WordDistance.WordDistance.FindShortestDistanceForWords(words, word1, word2)}");

words = new [] { "dog", "quick", "mouse", "fox", "dog", "over", "the", "lazy", "dog" };
word1 = "fox";
word2 = "dog";

Console.WriteLine(string.Join(',', words));
Console.WriteLine($"distance between {word1} and {word2} is: {WordDistance.WordDistance.FindShortestDistanceForWords(words, word1, word2)}");