namespace _6_1
//Ввести с клавиатуры предложение. Предложение представляет собой слова, разделенные пробелом. Знаки препинания не используются. Найти самое длинное слово в строке.
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Ввести предложение. Предложение представляет собой слова, разделенные пробелом. Знаки препинания не используются:");
            string input = Console.ReadLine();
            string[] words = input.Split(' ');
            string maxWord = "";
            foreach (string word in words)
            {
                if (word.Length > maxWord.Length)
                    maxWord = word;
            }
            Console.WriteLine($"Самое длинное слово в этом предложении: {maxWord}");
            Console.ReadKey();
        }
    }
}