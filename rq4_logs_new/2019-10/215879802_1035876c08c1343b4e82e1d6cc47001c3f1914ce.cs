using System;
using System.Linq;
using Trestlebridge.Interfaces;
using Trestlebridge.Models;
using Trestlebridge.Models.Plants;

namespace Trestlebridge.Actions {
     public class ChooseNaturalField
    {
        public static void CollectInput (Farm farm, ICompostProducing plant) {
            Console.Clear();
            if (farm.AvailableNaturalFields.Count() == 0) {
                Console.WriteLine("You need to create a Natural Field before you can purchase a plant.");
            }

            for (int i = 0; i < farm.NaturalFields.Count; i++)
            {
                if (farm.NaturalFields[i].GetCount() < farm.NaturalFields[i].Capacity) {
                    Console.WriteLine ($"{i + 1}. Natural Field ({farm.NaturalFields[i].GetCount()} plants)");
                    farm.NaturalFields[i].GroupPlants();
                }
            }

            Console.WriteLine ();

            // How can I output the type of animal chosen here?

            if (farm.AvailableNaturalFields.Count() != 0) {
                Console.WriteLine ($"Place the plant where?");
                Console.Write ("> ");
                int choice = Int32.Parse(Console.ReadLine ());
                    if (farm.NaturalFields[choice - 1].GetCount() < farm.NaturalFields[choice - 1].Capacity)
                    {
                        farm.NaturalFields[choice - 1].AddResource(plant);
                        if (farm.NaturalFields[choice - 1].GetCount() >= farm.NaturalFields[choice -1].Capacity) {
                            farm.AvailableNaturalFields.Remove(farm.NaturalFields[choice - 1]);
                        }
                        Console.WriteLine("Plant Added To Facility");
                    }
        }




            /*
                Couldn't get this to work. Can you?
                Stretch goal. Only if the app is fully functional.
             */
            // farm.PurchaseResource<IGrazing>(animal, choice);

        }
    }
}