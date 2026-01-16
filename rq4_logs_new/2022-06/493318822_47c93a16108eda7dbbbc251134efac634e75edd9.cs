namespace Hangman;

public class InvalidInputException : Exception
{

    public InvalidInputException()
    {

    }
    public InvalidInputException(string message) : base(message)
    {
        Console.WriteLine(message);
    }
    public InvalidInputException(string message, Exception exception) : base(message, exception)
    {

    }
}