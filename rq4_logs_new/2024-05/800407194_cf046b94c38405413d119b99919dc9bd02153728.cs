using System;
using System.Collections.Generic;
namespace winforms_chat
{
    public enum ChatCommand
    {
        Move,
        EndGame,
        Rematch,
        TimeSync,
    }
    public static class ChatCommandExt
    {
        private static readonly Dictionary<ChatCommand, string> commandMap = new Dictionary<ChatCommand, string>
        {
            { ChatCommand.Move, "MV#*" },
            { ChatCommand.EndGame, "ED#*" },
            { ChatCommand.Rematch, "RSTR#*" },
            { ChatCommand.TimeSync, "TSync#*" }
        };

        public static string ToString(this ChatCommand command)
        {
            return commandMap[command];
        }
    }
}