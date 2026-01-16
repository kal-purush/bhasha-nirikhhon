using System;

namespace Lessons
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello. Welcome at the base cource. Could you please fill your name:");
            string myName = Console.ReadLine();
            Console.WriteLine();

            Console.WriteLine("Ohhh.... " + myName + " is a cool name!");
            Console.WriteLine("Okey. Could you please fill your age:");
            string myAge = Console.ReadLine();
            Console.WriteLine();

            Console.WriteLine("What's your fave color?");
            string faveColor = Console.ReadLine();
            Console.WriteLine();

            Console.WriteLine("NEW ACCOUNT CREATED....");
            Console.WriteLine("Name: " + myName);
            Console.WriteLine("Age: " + myAge);
            Console.WriteLine("Fave color: " + faveColor);

            Console.ReadLine();
        }
    }
}