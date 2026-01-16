using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RaceTrack.RaceTrack.Cars
{
    class Mustang : RaceCar
    {
        public Mustang()
        {
            Name = "Mustang";
            TopSpeed = 120;
        }
        public override void StartEngine()
        {
            Console.WriteLine($"The {Name} roars to life!");
        }
    }
}