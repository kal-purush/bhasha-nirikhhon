using DesignPatterns.OOP.Polimorfism.Vehicles;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DesignPatterns.OOP.Polimorfism
{
    class Program
    {
        static void Main(string[] args)
        {
            var vehicles = new List<Vehicle>();
            vehicles.Add(new Car());
            vehicles.Add(new Bicycle());
            vehicles.Add(new Plane());

            foreach (var vehicle in vehicles)
            {
                vehicle.Move();
            }

            Console.ReadKey();
        }
    }
}