using System;
using System.Linq;
using Trestlebridge.Interfaces;
using Trestlebridge.Models;
using Trestlebridge.Models.Plants;

namespace Trestlebridge.Actions
{
    public class ChooseMeatProcessor
    {
        public static void CollectInput(Farm farm)
        {
            Console.Clear();
            for (int i = 0; i < farm.GrazingFields.Count; i++)
            {
                if (farm.GrazingFields[i].GetCount() < farm.GrazingFields[i].Capacity)
                {
                    Console.WriteLine($"{i + 1}. Grazing Field ({farm.GrazingFields[i].GetCount()} animals)");

                }
                Console.WriteLine();

                    if (farm.GrazingFields.Count() != 0)
                    {
                        Console.WriteLine($"Which Facility has the animals you want to process?");
                        Console.Write("> ");
                        int choice = Int32.Parse(Console.ReadLine());
                        if (choice <= farm.GrazingFields.Count()){
                            Console.Clear();
                            farm.GrazingFields[choice-1].GetAnimals(farm.GrazingFields[choice-1]);

                        }
                    }

                }
    }
    }}