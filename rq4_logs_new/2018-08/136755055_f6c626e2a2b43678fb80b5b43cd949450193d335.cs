using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace switchStatement.Interfacing
{
    public class  Dog : INterFaceClass, IElephant
    {
        public void CallingAnimal()
        {
            Console.WriteLine("The name of my Dog is Snoopy");
        }

        public void CallingAnimal(string animalName)
        {
            Console.WriteLine("The name of my DOGG is {0}", animalName);
        }

        public void Animallegs(int legs)
        {
            Console.WriteLine("My dog has {0} legs", legs);
        }

        public void CallingEle(string animalName)
        {
            Console.WriteLine("My elephant is called {0}", animalName);
        }

        public void Elelegs(int legs)
        {
            Console.WriteLine("My elephant has {0} legs", legs);
        }

        public void StrangeAnimal()
        {
            Console.WriteLine("This animal only reside in the Dog Class");
        }
    }
}