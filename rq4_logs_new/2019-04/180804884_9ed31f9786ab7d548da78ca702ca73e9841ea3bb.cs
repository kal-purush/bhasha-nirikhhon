using System;

namespace AlTextAdventure
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Welcome to The Bar!");
            Console.WriteLine("Would you like a table?");

            string response = Console.ReadLine();

            if (response == "Yes")
            {
                Console.WriteLine("Great! Would you like a drink?");


                if (response == "Yes")
                {
                    Console.WriteLine("Fantastic, here you go!");
                }

                if (response == "No")
                {
                    Console.WriteLine("Ok, you need to get out!");
                }

            }
            else if (response == "No")
            {
                Console.WriteLine("Ok, Get out this area is for customers only!");
            }

            else
            {
                Console.WriteLine("Ok, here's your check!");
            }
        }
    }
}