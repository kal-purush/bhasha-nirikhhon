using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CSharp_OOP_Exercise
{
    public abstract class Bird : IDance
    {
        public string Name { get; private set; }
        public string Color { get; private set; }

        public Bird(string name, string color)
        {
            Name = name;
            Color = color;
        }

        // Commented to check the abstract function of the class
        /*
        public void Speak()
        {
            Console.WriteLine($"My name is {Name} and I am a {Color} bird.");
        }
        
        public void Fly()
        {
            Console.WriteLine($"I'm {Name} and I can fly high in the blue sky!");
        }
        */

        //This method is marked with Abstract keyword
        //This means that any classes extending on Bird must implement this
        //Abstract methods don't have a method body
        public abstract void Speak();

        public virtual void Fly()
        {
            //Default implementation
            Console.WriteLine("Hi there! I can fly.");
        }

        //Implementing interface methods
        public void Spin()
        {
            Console.WriteLine($"{Name} spin!");
        }

        public void DoTheCaterpillar()
        {
            Console.WriteLine($"{Name} do the wriggly woo!");
        }

        public void Jump()
        {
            Console.WriteLine($"{Name} jump in the air!");
        }

    }
}