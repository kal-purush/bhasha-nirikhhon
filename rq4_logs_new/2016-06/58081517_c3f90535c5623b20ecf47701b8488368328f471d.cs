using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ColdPlayProject.interfaceNamespace.implementation
{
    public class HondaCivic : ICar
    {

        public void PlayMusic()
        {
            Console.WriteLine("Honda Can play music");
        }

        public void Move()
        {
            string carName = "This is Honda Civic";
            Console.WriteLine("Our car name is {0}", carName);
        }

        public double CalculateDistanceAfterRace()
        {
            double price = 45d;
            double n = price*3;
            return n;
        }

        public string DisplayCarName(string carMake)
        {
            string name = "Camry Car";
            return name;
        }

        public bool HasStopped()
        {
            return true;
        }

        public void ShowVehicleName()
        {
            Console.WriteLine("Name is Honda");
        }
    }
}