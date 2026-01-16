using System;

namespace Method_excorsize
{
    internal class Program
    {
        static void Main(string[] args)
        {
            
            Console.WriteLine("What is your name?");
            var  personName =  Console.ReadLine();

            Console.WriteLine("whats your favorite color");
            var favClr  = Console.ReadLine();

            //          <name>
            //
            //          <color>
            //

            Console.WriteLine(personName + "favorite sport  is" + favClr);


        }
    }
}