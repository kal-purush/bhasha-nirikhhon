using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microwave.Classes.Interfaces;

namespace Microwave.Classes.Boundary
{
    class Buzzer : IBuzzer
    {
        private IOutput myOutput;
        private bool isOn = false;

        public Buzzer(IOutput output)
        {
            myOutput = output;
        }
        public void TurnOn()
        {
            if (!isOn)
            {
                myOutput.OutputLine("beep beep beep");
                isOn = true;
            }
        }

        public void TurnOff()
        {
            if (isOn)
            {
                myOutput.OutputLine("*silence*");
                isOn = false;
            }
        }
    }
}