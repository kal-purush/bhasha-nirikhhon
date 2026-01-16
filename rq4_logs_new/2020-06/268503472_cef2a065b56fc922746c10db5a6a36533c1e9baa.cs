using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace RobotFactory
{
    class Program
    {
        static void Main(string[] args)
        {
            var factory = RobotFactory.Instance;
            var robots = new List<Robot>();
            var random = new Random();

            var numberOfRobots = random.Next(10, 20);
            for (int i = 0; i < numberOfRobots; i++)
            {
                robots.Add(factory.CreateRobot());
            }

            Console.WriteLine("Created robots");
            robots.ForEach(Console.WriteLine);

            Console.WriteLine("\nFirst and last robot before/after reset");
            Console.WriteLine(robots[0].ToString());
            factory.ResetRobot(robots[0]);
            Console.WriteLine(robots[0].ToString());
            
            Console.WriteLine();
            
            Console.WriteLine(robots[numberOfRobots - 1].ToString());
            factory.ResetRobot(robots[numberOfRobots - 1]);
            Console.WriteLine(robots[numberOfRobots - 1].ToString());

            Console.WriteLine($"\nAfter creating {numberOfRobots} robots, and resetting two of these, total number of stored identifiers should be {numberOfRobots+2}");
            Console.WriteLine($"Total number of stored identifiers is {factory.CreatedRobotsNames.Count}");

        }
    }
}