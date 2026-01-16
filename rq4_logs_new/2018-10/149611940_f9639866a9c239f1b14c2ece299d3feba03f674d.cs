using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp1
{
    class Program
    {
        static void Main(string[] args)
        {


            //bool shouldContinue;
            //do
            //{

           
            bool shouldContinue;


            Console.WriteLine("Welcome to Gand Circus' Room Detail Generator");

            do
            {
                // prompt user
                Console.WriteLine("Enter length of room :");
                double lengthOfRoom = double.Parse(Console.ReadLine());

                Console.WriteLine("Enter width of room :");
                double widthOfRoom = double.Parse(Console.ReadLine());

                //formulas
                double areaOfRoom = lengthOfRoom * widthOfRoom;
                double perimeterOfRoom = 2 * (lengthOfRoom * widthOfRoom);

                //calculations
                Console.WriteLine("The area of your room is: {0}", areaOfRoom);
                Console.WriteLine("The perimeter of your rooms is: {0}", perimeterOfRoom);

                Console.WriteLine("Would you like to continue? ");
                if (Console.ReadLine() == "yes ")

                {
                    shouldContinue = true;
                }
                else
                {
                    shouldContinue = false;
                }
            }
            while (true);
            


        }
    }

    }


