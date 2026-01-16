using System;

class Program
{
    static void Main()
    {
        GetUserInputs();
        Console.ReadLine();
    }

    static void GetUserInputs()
    {
        string[] responses = { "Yes", "No", "Maybe", "Ask again later" };
        
        Console.Write("What's your question? ");
        string question = Console.ReadLine();
        
        Random random = new Random();
        int randomIndex = random.Next(responses.Length);
        string answer = responses[randomIndex];
        
        Console.WriteLine(answer);
    }
}