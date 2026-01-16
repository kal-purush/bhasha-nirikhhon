using System.Collections.Generic;

namespace MononokeEngine.Utils.Terminal
{
    public class TerminalManager
    {
        
        
        public bool PauseGameOnOpen { get; set; }

        private Dictionary<string, ITerminalCommand> _commands;
        
        
        

        internal TerminalManager()
        {
            _commands = new Dictionary<string, ITerminalCommand>();
        }
        
        
        
        public void AddCommand(string name, ITerminalCommand command)
        {
            // TODO
        }


        public void Open()
        {
            // TODO
        }
        
        public void Close()
        {
            // TODO
        }


        
        internal void Execute(string line)
        {
            string[] split = line.Split(' ');
            
            string cmd = split[0];
            
            string[] args = new string[split.Length-1];
            for (int i = 1; i < split.Length; i++)
                args[i - 1] = split[i];

            ITerminalCommand command = _commands[cmd];
            command.Execute(args);
        }
        
        
        
        
    }
}