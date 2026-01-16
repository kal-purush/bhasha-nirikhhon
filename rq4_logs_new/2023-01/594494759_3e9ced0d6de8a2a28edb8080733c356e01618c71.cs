namespace MDConsole;

public sealed class ConsoleOption
{
    private const string COMMAND_PREFIX = "-";
    private const string COMMAND_FULL_PREFIX = "--";

    public ConsoleOption(string command, string commandFull)
	{
        Command = command;
        CommandFull = commandFull;
    }


    public string Command { get; }
    public string CommandFull { get; }
    public string Value { get; private set; }

    public List<string> GetOptions()
    {
        var result = new List<string>();
        if (!string.IsNullOrWhiteSpace(Command))
            result.Add($"{COMMAND_PREFIX}{Command.ToLower()}");
        if (!string.IsNullOrWhiteSpace(CommandFull))
            result.Add($"{COMMAND_FULL_PREFIX}{CommandFull.ToLower()}");
        return result;
    }

    public ConsoleOption SetValue(string value)
    {
        Value = !string.IsNullOrWhiteSpace(value) ? value : null;
        return this;
    }
}