using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ObjectsReview
{
    class Animal
    {
        // Auto-Impl Property for trivial get and set
        public String name;
        public int age;

        // Class constructor
        public Animal(String name)
        {
            this.name = name;
            this.age = 0;
        }

        // Generic behavior with virtual method 
        // Each child class needs to override to change behavior 
        public virtual void makeNoise()
        {
            Console.WriteLine("...");
        }
    }
}