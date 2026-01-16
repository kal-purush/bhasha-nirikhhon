using System;
using System.Collections.Generic;
using System.Text;

namespace DotNetMotors.classes
{
    class Coupe : RunningCar
    {
        public override bool Starts { get ; set ; }
        public override bool MakesNoise { get ; set ; }
        public override bool IsReal { get; set; }
        
        public override void DriveForward()
        {
            Console.WriteLine("The sedan drives forward");
        }
        public override void Honk()
        {
            Console.WriteLine("BEEP BEEP");
        }

        public override void Reverse()
        {
            Console.WriteLine("The car drives forward");
        }

        public override void TurnOff()
        {
            Console.WriteLine("The sedan has turned off");
        }

        public override void TurnOn()
        {
            Console.WriteLine("The sedan has turned on");
        }
    }
}