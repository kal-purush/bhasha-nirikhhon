class Program
{
    static void Main()
    {
         Console.WriteLine("Let's play Guess the Number. ");
         int difficulty = GetDifficultyLevel();

         do
         {
             PlayGame(difficulty);
         } while (PlayAgain());
         
         Console.WriteLine("Thanks for playing! ");
    }

    static int GetDifficultyLevel()
    {
        int difficulty;
        do
        {
            Console.WriteLine("Pick a difficulty level (1, 2, or 3): ");
        } while (!int.TryParse(Console.ReadLine(), out difficulty));
        return difficulty;
    }

    static void PlayGame(int difficulty)
    {
        Random random = new Random();
        int maxNumber = (int)Math.Pow(10, difficulty);
        int targetNumber = random.Next(1, maxNumber + 1);
        int guess;
        int attempts = 0;
        
        Console.WriteLine($"I have my number between 1 and {maxNumber}. What's your guess?");

        do
        {
            Console.Write("What's your guess? ");
        } while (!int.TryParse(Console.ReadLine(), out guess) || guess < 1 || guess > maxNumber);

        while (guess != targetNumber)
        {
            if (guess < targetNumber)
                Console.WriteLine("Too low, guess again! ");
            else
                Console.WriteLine("Too high, guess again! ");
            while (!int.TryParse(Console.ReadLine(), out guess) || guess < 1 || guess > maxNumber)
            {
                Console.WriteLine("Invalid input, please enter a valid guess.");
            }

            attempts++;
        }

        attempts++;
        Console.WriteLine($"You got it in {attempts} {(attempts == 1 ? "guess" : "guesses")}!");
        ProvideResultsOfAttempts(attempts);
    }

    static bool PlayAgain()
    {
        Console.Write("Would you like to play again? (y/n) ");
        return Console.ReadLine().Trim().ToLower() == "y";
    }

    static void ProvideResultsOfAttempts(int attempts)
    {
        if (attempts == 1)
            Console.WriteLine("First try, congrats!");
        else if (attempts >= 2 && attempts <= 4)
            Console.WriteLine("Thats okay..");
        else if (attempts >= 4 && attempts <= 6)
            Console.WriteLine("You can do better than that! ");
    }
}