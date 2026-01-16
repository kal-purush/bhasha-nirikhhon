using System;

namespace FirstClass;
class Program
{
    static void Main(string[] args)
    {
        Car audi = new Car("audi");
        audi.Drive();
        Car bmw = new Car("bmw");

        Console.WriteLine("if you want the car to stop write down'stop'.");

        string userInput = Console.ReadLine();
        if (userInput == "stop" || userInput == "Stop")
        {
            audi.Stop();
        }
        else
        {
            audi.Drive();
        }

        Console.Read();
    }
}