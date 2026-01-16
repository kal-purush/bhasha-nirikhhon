using System;

namespace ToyShop1
{
    class Program
    {
        static void Main(string[] args)
        {
            const double puzzelPrice = 2.60;
            const double talkingDollsPrice = 3;
            const double teddyBearPrice = 4.10;
            const double minionPrice = 8.20;
            const double truckPrice = 2;

            double priceForExcursion = double.Parse(Console.ReadLine());
            int countOfPuzzels = int.Parse(Console.ReadLine());
            int countOfTalkingDolls = int.Parse(Console.ReadLine());
            int countOfTeddyBears = int.Parse(Console.ReadLine());
            int countOfMinions = int.Parse(Console.ReadLine());
            int countOfTruck = int.Parse(Console.ReadLine());

            double sum = (countOfPuzzels * puzzelPrice) + (countOfTalkingDolls * talkingDollsPrice) +
                (countOfTeddyBears * teddyBearPrice) + (countOfMinions * minionPrice) +
                (countOfTruck * truckPrice);
            double countOfToys = countOfPuzzels + countOfTalkingDolls + countOfTeddyBears + countOfMinions +
                countOfTruck;

            double discount = 0;
            if(countOfToys>=50)
            {
                discount = sum * 0.25;
            }
            double finalSum = sum - discount;
            double rentForBuilding = finalSum * 0.10;
            double rent = finalSum - rentForBuilding;
            double moneyMade = finalSum - rent;
            if (priceForExcursion > moneyMade)
            {
                double moneyneeded = priceForExcursion - moneyMade;
                Console.WriteLine($"Not enough money!{moneyneeded:f2}lv needed.");
            }
            else
            {
                double moneyLeft = moneyMade - priceForExcursion;
                Console.WriteLine($"Yes! {moneyLeft:f2} lv left");
            }

        }
    }
}