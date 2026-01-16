namespace Commands;

/// <summary>
///     Функция обратного вызова, если введённая команда не найдена.
/// </summary>
/// <param name="commandName">Имя команды.</param>
public delegate void CommandNotFound(string commandName);

/// <summary>
///     Класс для создания интерфейса командной строки.
/// </summary>
public class CommandLineInterface : CommandManager
{
    /// <summary>
    ///     Функция обратного вызова, если введённая команда не найдена.
    /// </summary>
    public CommandNotFound? OnCommandNotFound;

    /// <summary>
    ///     Префикс поля ввода.
    /// </summary>
    public char PromptPrefix;

    /// <summary>
    ///     Инициализирует интерфейс.
    /// </summary>
    public CommandLineInterface() : base()
    {
    }

    /// <summary>
    ///     Инициализирует интерфейс, задавая префикс поля ввода.
    /// </summary>
    /// <param name="promptPrefix">Префикс поля ввода.</param>
    public CommandLineInterface(char promptPrefix) : base()
    {
        PromptPrefix = promptPrefix;
    }

    /// <summary>
    ///     Читает и обрабатывает ввод пользователя,
    ///     вызывает соответствующие команды.
    /// </summary>
    public void ProcessInput()
    {
        if (PromptPrefix != 0)
            Console.Write(PromptPrefix);

        string? input = Console.ReadLine()?.Trim();

        if (input == null || input == string.Empty)
            return;

        (string name, string arguments) = ParseInput(input);

        Command? command = GetCommand(name);

        if (command == null)
        {
            if (OnCommandNotFound != null)
                OnCommandNotFound(name);

            return;
        }

        command.Execute(arguments);
    }

    /// <summary>
    ///     Разбивает ввод на имя команды и её аргументы.
    /// </summary>
    /// <param name="input">Ввод.</param>
    /// <returns>Имя и аргументы.</returns>
    private static (string name, string arguments) ParseInput(string input)
    {
        string name, arguments;

        int nameEnd = input.IndexOf(' ');

        if (nameEnd == -1)
        {
            name = input;
            arguments = string.Empty;
        }
        else
        {
            name = input.Substring(0, nameEnd);
            arguments = input.Substring(nameEnd + 1).TrimStart();
        }

        return (name, arguments);
    }
}