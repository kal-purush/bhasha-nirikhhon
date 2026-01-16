using System;
using System.Collections.Generic;

static class CommandFactory
{
    public static Command CreateCommand(Type commandType, Dictionary<string, int> parameters)
    {
        Command cmd = (Command)Activator.CreateInstance(commandType);
        cmd.Initialize(parameters);

        return cmd;
    }
}