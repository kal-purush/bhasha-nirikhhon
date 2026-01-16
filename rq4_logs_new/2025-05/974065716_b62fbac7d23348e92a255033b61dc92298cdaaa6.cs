using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EasySave.Helpers;

public delegate void CommandHandler(Command command);
public delegate List<string> CommandArgumentsParser(string commandLine);

public class Command {
    public required string Name { get; set; }
    public List<string> Arguments { get; set; } = [];
    public CommandArgumentsParser ArgumentsParser { get; set; } = (commandLine) => {
        return [.. commandLine.Split(' ')];
    };
    public required CommandHandler Handler { get; set; }

    public virtual Command Clone() {
        return new Command {
            Name = this.Name,
            Arguments = [.. this.Arguments],
            ArgumentsParser = this.ArgumentsParser,
            Handler = this.Handler
        };
    }

    public void Run() {
        this.Handler(this);
    }

    public void Run(List<string> arguments) {
        this.Arguments = arguments;
        this.Run();
    }
}

public class Commands {
    private readonly Dictionary<string, Command> _commands = [];

    public void RegisterCommand(Command command) {
        this._commands[command.Name] = command;
    }
    public Command RegisterCommand(string name, CommandHandler handler, CommandArgumentsParser? commandArgumentsParser = null) {
        Command command = new() {
            Name = name,
            Handler = handler,
            ArgumentsParser = commandArgumentsParser ?? ((string commandLine) => {
                return [.. commandLine.Split(' ')];
            }),
            Arguments = []
        };
        RegisterCommand(command);
        return command;
    }

    public Command? ParseCommand(string commandLine) {
        string commandName = commandLine.Split(' ')[0];
        if (this._commands.TryGetValue(commandName, out Command? command)) {
            command.Arguments = command.ArgumentsParser(commandLine);
            return command;
        } else {
            return null;
        }
    }

    public void RunCommand(string commandLine) {
        Command? command = this.ParseCommand(commandLine) ?? throw new ArgumentException($"Command '{commandLine}' not found.");
        this.RunCommand(command);
    }
    public void RunCommand(string name, string arguments) {
        Command? command = this._commands.GetValueOrDefault(name) ?? throw new ArgumentException($"Command '{name}' not found.");
        command.Arguments = command.ArgumentsParser(arguments);
       this.RunCommand(command);
    }
    public void RunCommand(string name, List<string> arguments) {
        Command? command = this._commands.GetValueOrDefault(name) ?? throw new ArgumentException($"Command '{name}' not found.");
        command.Arguments = arguments;
        this.RunCommand(command);
    }
    public void RunCommand(Command command) {
        if (this._commands.ContainsKey(command.Name)) {
            command.Handler(command);
        } else {
            throw new ArgumentException($"Command '{command.Name}' not found.");
        }
    }

    public Command GetCommand(string name) {
        if (this._commands.TryGetValue(name, out Command? command)) {
            return command.Clone();
        } else {
            throw new ArgumentException($"Command '{name}' not found.");
        }
    }
}
