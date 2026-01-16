using LagDaemon.YAMUD.Model.Utilities;
using System;
using System.Linq;

namespace LagDaemon.YAMUD.Model.GameCommands
{
    public class GoCommand : Command
    {
        public Direction Direction { get; set; }

        public override bool Validate(IGameContext context)
        {
            if (Parameters.Count != 1)
            {
                return false;
            }
            else
            {
                Direction direction;
                Enum.TryParse(Parameters[0], true, out direction);
                Direction = direction;
                return true;
            }
        }

        public override string Execute(IGameContext context)
        {
            return "Go Command Executed";
        }
    }
}