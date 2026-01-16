using System;
using Trestlebridge.Models;
using Trestlebridge.Models.Animals;

namespace Trestlebridge.Actions
{
    internal class ChooseChickenHouse
    {
        public static void CollectInput (Farm farm, Chicken chicken) {
            Console.Clear();

            for (int i = 0; i < farm.ChickenHouses.Count; i++)
            {
                Console.WriteLine ($"{i + 1}. Chicken House");
            }

            Console.WriteLine ();

            // How can I output the type of animal chosen here?
            Console.WriteLine ($"Place the Chicken where?");

            Console.Write ("> ");
            int choice = Int32.Parse(Console.ReadLine ());

            bool atCapacity = true;

            while (atCapacity == true)
            {
                if (farm.ChickenHouses[choice - 1].GetChickenCount() < farm.ChickenHouses[choice - 1].Capacity)
                {
                    atCapacity = false;
                    farm.ChickenHouses[choice - 1].AddResource(chicken);
                    Console.WriteLine("Chicken Added To Facility");
                }
                else
                {
                    atCapacity = true;
                    Console.WriteLine("Too many chickens. Choose another facility.");
                    Console.Write ("> ");
                    choice = Int32.Parse(Console.ReadLine ());
                }
            }
            // else return user back to list of facilities (do this after lunch)


            /*
                Couldn't get this to work. Can you?
                Stretch goal. Only if the app is fully functional.
             */
            // farm.PurchaseResource<IGrazing>(animal, choice);

        }
    }
}