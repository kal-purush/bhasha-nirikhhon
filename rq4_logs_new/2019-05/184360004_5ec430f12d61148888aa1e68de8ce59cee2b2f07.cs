using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace AbstractFactoryPatternExample
{
   public static class AbstractFactoryPatternSim
   {
public static void Run()
{
   Console.WriteLine("***Abstract Factory Method Pattern***");

   var collegeSim = new CollegeFootballGameSim();
   var nflSim = new NflFootballGameSim();

   for (int i = 0; i < 3; i++)
   {
      collegeSim.Run((OffensiveType) i);
      nflSim.Run((OffensiveType)i);
   }
}
   }
}