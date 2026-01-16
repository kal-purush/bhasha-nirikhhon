using System;

namespace SingletonPattern
{
    internal static class Program
    {
        private static void Main(string[] args)
        {
            var firstSingleton = Singleton.CreateInstance("Little singleton");
            var secondSingleton = Singleton.CreateInstance("Another singleton");

            Console.WriteLine("First and second instance of the Singleton are {0}",
                firstSingleton == secondSingleton ? "identical" : "not identical");
            
            Console.WriteLine("First singleton name is {0}", firstSingleton.GetName());
            Console.WriteLine("Second singleton name is {0}", secondSingleton.GetName());
        }
    }
}