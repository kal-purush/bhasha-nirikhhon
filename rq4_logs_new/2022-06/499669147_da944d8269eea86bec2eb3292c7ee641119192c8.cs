using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NbodyWithUI.Models
{
    static class OptionsMenu
    {
        public static int TotalSimTime = 600000;
        public static int Resolution = 30;
        public static int NumberOfParticles = 3;
        public static bool StartSim = false;
        public static bool Quit = false;

        //public OptionsMenu(int totalSimTime, int resolution, int numberOfParticles)
        //{
        //    TotalSimTime = totalSimTime;
        //    Resolution = resolution;
        //    NumberOfParticles = numberOfParticles;
        //}
        public static void printMenu()
        {
            Console.WriteLine("options (no input parsing yet, only input positive integers):");
            Console.WriteLine("1. Set the total sim time, currently: " + TotalSimTime);
            Console.WriteLine("2. Set the resolution, currently: " + Resolution);
            Console.WriteLine("3. (IS NOT YET IMPLEMENTED)Set the amount of randomized particles, currently: " + NumberOfParticles);
            Console.WriteLine("9. Quit the program");
            Console.WriteLine("4. START SIM!!!");
            Console.WriteLine("\n");

            string input = Console.ReadLine();
            if (input == "1")
            {
                Console.Write($"Change current value from {TotalSimTime} to: ");
                string value = Console.ReadLine();
                TotalSimTime = Int32.Parse(value);

            }
            else if (input == "2")
            {
                Console.Write($"Change current value from {Resolution} to: ");
                string value = Console.ReadLine();
                Resolution = Int32.Parse(value);

            }
            else if (input == "3")
            {
                Console.Write($"Change current value from {NumberOfParticles} to: ");
                string value = Console.ReadLine();
                NumberOfParticles = Int32.Parse(value);

            }
            else if (input == "4")
            {
                StartSim = true;
            }
            else if (input == "9")
            {
                Quit = true;
            }
        }
    }
}