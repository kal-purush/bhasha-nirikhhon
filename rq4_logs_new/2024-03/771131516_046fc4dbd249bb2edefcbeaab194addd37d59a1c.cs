using System.Runtime.ExceptionServices;

internal class Program
{
    private static void Main(string[] args)
    {
        string userName;
        string userInput;
        int score = 0;
        int numberOfFlips; 
        Random coinFlip = new Random();
        int result = coinFlip.Next(0, 2);

        Console.WriteLine("Welcome to the coin flip challange.");
        Console.WriteLine("What is your name?");
        userName = Console.ReadLine();
        Console.WriteLine("Welcome " + userName + ". Do you want to do the Coin Flip Challange? Yes/No");
        userInput = Console.ReadLine();
        if (userInput == "Yes")
        {
            for (numberOfFlips = 0; numberOfFlips <6; numberOfFlips++)
            {

            }
        }
        else
        {
            Console.WriteLine("You are a coward" + userName +".");
        }


    }
}