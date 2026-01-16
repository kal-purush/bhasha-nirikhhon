using System;
using System.Collections.Generic;
using DecoratorPattern.Components;
using DecoratorPattern.Components.Decorators;

namespace DecoratorPattern
{
    internal static class Program
    {
        private static void Main(string[] args)
        {
            var coffee = new Coffee();

            var beverages = new Dictionary<string, AbstractBeverage>
            {
                {"Regular coffee", coffee},
                {"Caramel flavoured coffee", new CaramelFlavourCoffee(coffee)},
                {"Chocolate flavoured coffee", new ChocolateFlavourCoffee(coffee)},
                {"Coffee with whipped cream", new WhippedCreamCoffee(coffee)},
                {
                    "Coffee with everything",
                    new CaramelFlavourCoffee(new ChocolateFlavourCoffee(new WhippedCreamCoffee(coffee)))
                }
            };

            foreach (var beverage in beverages)
            {
                Console.WriteLine("{0} costs {1}€", beverage.Key, beverage.Value.GetPrice());
            }
        }
    }
}