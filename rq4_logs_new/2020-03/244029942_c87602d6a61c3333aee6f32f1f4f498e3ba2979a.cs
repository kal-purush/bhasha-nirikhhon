using System;

namespace HelloWorld
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Howdy Y'all!!");
            Console.ReadKey();

            var animals = new string[] { "Triceratops", "Gorilla", "Corgi", "Toucan" };
            
            var vowels = new[] { 'a', 'e', 'i', 'o', 'u', 'y' };
            foreach (var animal in animals)
            {
                var numberOfVowels = 0;
                foreach (var animalChar in animal)
                {
                    foreach (var vowel in vowels)
                    {
                        if (vowel == animalChar)
                        {
                            numberOfVowels++;
                        }
                    }
                }
                if (numberOfVowels >= 2)
                {
                    Console.WriteLine(animal);
                    Console.ReadKey();
                }
            }
        }
    }
}