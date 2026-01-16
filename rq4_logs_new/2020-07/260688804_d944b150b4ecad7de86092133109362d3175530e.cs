using System;
using System.Collections.Generic;
using System.Linq;
using pizza_commands.BusinessLogic.Interfaces;

namespace pizza_commands.BusinessLogic
{
    public class CommandsBL: ICommandsBL
    {
         // Don't do that it's ugly.
        // Just for the demo
        private List<Command> commands;


        public CommandsBL()
        {
            Console.WriteLine("*********** Cnstruct");
            var rng = new Random();
            this.commands = Enumerable.Range(1, 5).Select(index => new Command
            {
                Date = DateTime.Now,
                Id = index,
                Pizzas = new[] { rng.Next(1, 10), rng.Next(1, 10) }
            })
            .ToList();
        }

        public Command AddCommand(Command command)
        {
            this.commands.Add(command);
            return command;
        }

        public List<Command> GetCommands()
        {
            return this.commands;
        }

        public Command GetCommand(int commandId)
        {
            return this.commands.FirstOrDefault(command => command.Id == commandId);
        }

    }
}