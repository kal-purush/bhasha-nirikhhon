using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace switchStatement
{
    class CallConstructor : ConstructorMe
    {
        public CallConstructor(string animalType, int eyes, int legs) : base(animalType, eyes, legs)
        {
        }

        public CallConstructor(string animalType, int eyes, int legs, int tail) : base(animalType, eyes, legs, tail)
        {
        }
        public void CreateAnimal()
        {
            Species = "Dog";
            numberOfEyes = 2;
            numberOfLegs = 4;

            Console.WriteLine("A {0} has {1} eyes & {2} legs", Species, numberOfEyes, numberOfLegs);
        }

       
    }

}