using System;
using System.Collections.Generic;
using System.Text;

namespace PokemonZoo.cs
{
    abstract class Firetype : Pokemon , ICatchable
    {
        //Firetype properties
        public override string Weakness { get; set; } = "This pokemon weakness is Water";

        public string Catch()
        {
            Console.WriteLine("this Fire type pokemon is Catchable");
            return "yes";
        }

        //Firetype methods
        public virtual string EmberAttack()
        {
            Console.WriteLine("This pokemon Embers at the fire");
            return "Embers";
        }

        public virtual string Flying()
        {
            Console.WriteLine("This Pokemon is Flying");
            return "Spread Wings and Flys";
        }
    }
}