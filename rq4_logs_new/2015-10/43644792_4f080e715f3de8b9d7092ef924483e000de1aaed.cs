using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApplication1
{
    class Animal {
        public Animal(string name)
        {
            Name = name;
        }
        virtual public string AnimalType { get; private set; }
        public string Name { get; private set; }
    }

    class Dog : Animal {
        public Dog(string name):base(name)
        {

        }
        public override string AnimalType
        {
            get
            {
                return "dog";
            }
        }
    }

    class Cat : Animal
    {
        public Cat(string name):base(name)
        {

        }
        public override string AnimalType
        {
            get
            {
                return "cat";
            }
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            List<Animal> animals = new List<Animal>()
            {
                new Dog("dog1"),
                new Dog("dog2"),
                new Cat("cat1")
            };

            foreach (var animal in animals)
            {
                if (animal.AnimalType == "dog")
                {
                    Console.WriteLine(animal.Name + "汪汪");
                } else if (animal.AnimalType == "cat")
                {
                    Console.WriteLine(animal.Name + "喵喵");
                }
            }
        }
    }


}