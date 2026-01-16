
namespace BudgetTourist.Engines
{
    using BudgetTourist.HousingContracts.Contracts;
    using BudgetTourist.Models.HousingModels;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    public static class Engine
    {
        public static void Run()
        {
            // TODO
            ICollection<IHousing> listHotels = HotelFactory.GetHotels();

            //Ask for money
            Console.WriteLine("How much money do you have to spend?");
            string inputCash = Console.ReadLine();
            if (string.IsNullOrEmpty(inputCash))
            {
                throw new ArgumentNullException();
            }
            decimal userCash = decimal.Parse(inputCash);
            if (userCash >= 0 && userCash < 10)
            {
                Console.WriteLine("we suggest you to go to tent under the stars");
            }
            else if (userCash > 10)
            {
                CheckHotel(listHotels, userCash);
            }

            //Hotels
            //var result = listHotels.ToArray();
            //var newResult = result.Select(h => h.Price <= userCash);



            //Create restaurants

        }

        private static void CheckHotel(ICollection<IHousing> listHotels, decimal userCash)
        {
            foreach (var item in listHotels)
            {
                if (item.Price < userCash)
                {
                    Console.WriteLine(item);
                    Console.WriteLine("==================================================");
                }
            }
        }

    }
}