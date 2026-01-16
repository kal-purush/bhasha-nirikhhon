using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace FactoryPattern.AnimalFactories
{
    public class RandomAnimalFactory : SimpleAnimalFactory
    {
        protected List<string> AnimalList;

        public RandomAnimalFactory()
        {
            AnimalList = GetRandomAnimals();
        }
        
        public override IAnimal CreateAnimal(string animal = null)
        {
            var random = new Random();
            var animalClass = AnimalList[random.Next(AnimalList.Count)];
            
            return base.CreateAnimal(animalClass);
        }
        
        private static List<string> GetRandomAnimals()
        {
            var asm = Assembly.GetExecutingAssembly();

            var animals = asm.GetTypes()
                .Where(type => type.Namespace == "FactoryPattern.Animals")
                .Select(type => type.Name);

            return animals.ToList();
        }
    }
}