namespace RuleEngineTester.RuleEngine.ErrorHandling;

public class RuleEngineException : Exception
{
    public RuleEngineException(string message) : base(message)
    {
        LogException(message);
    }

    private void LogException(string message)
    {
        Console.WriteLine($"Logging Exception: {message}");
    }
}


public class ParseRuleException : RuleEngineException
{
    public ParseRuleException(string textToParse, string pattern, string message) : base(message)
    {
        Console.WriteLine($"Text to Parse: {textToParse}");
        Console.WriteLine($"With Pattern: {pattern}");
    }
}