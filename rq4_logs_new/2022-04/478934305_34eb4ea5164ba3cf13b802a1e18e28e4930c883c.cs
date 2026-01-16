using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microwave.Classes.Interfaces;

namespace Microwave.Classes.Boundary
{
    class Display : IDisplay
    {
        private IOutput myOutput;

        public Display(IOutput output)
        {
            myOutput = output;
        }

        public void ShowTime(int min, int sec)
        {
            myOutput.OutputLine($"Display shows: {min:D2}:{sec:D2}");
        }

        public void ShowPower(int power)
        {
            myOutput.OutputLine($"Display shows: {power} W");
        }

        public void Clear()
        {
            myOutput.OutputLine($"Display cleared");
        }
    }
}