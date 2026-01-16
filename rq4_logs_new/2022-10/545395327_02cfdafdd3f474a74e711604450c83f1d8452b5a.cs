namespace CollabApp;

public class Program
{
    static void Main(string[] args)
    {
        Console.WriteLine("Guess what number I'm thinking of!");
        Random r = new();
        int numberToGuess = r.Next(0, 10);
        bool guessing = true;

        while(guessing)
        {
            string guess = Console.ReadLine();

            if (guess == numberToGuess.ToString()) break;
            Console.WriteLine("Guess again!");
        }
        Console.WriteLine($"Well done! It was {numberToGuess}");
    }
}