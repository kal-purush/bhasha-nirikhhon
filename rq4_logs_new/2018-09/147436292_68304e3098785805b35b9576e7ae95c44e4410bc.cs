using System;
using System.Collections.Generic;
using System.Text;

namespace legoMiniFigures.Legs
{
    class BabyLegs : Legs
    {
        public BabyLegs()
        {
            Length = 10;
            HasPants = false;
        }

        public override void Walk()
        {
            if (Length > 18)
                Console.WriteLine($"The {MainColor} baby legs strutted around the playpen in {ShoeColor} shoes.");

            else
                Console.WriteLine($"the {MainColor} baby legs crawled around the playpen");

        }

        public override void Kick(MiniFigure minifigure)
        {
            Console.WriteLine($"Baby legs kicked {minifigure.Name}, the {minifigure.Description}.");
        }
    }

    class Legs
    {
        public int Length { get; set; } = 34;
        public bool HasPants { get; set; } = true;
        public string ShoeColor { get; set; }
        public string MainColor { get; set; }

        public virtual void Walk()
        {
            Console.WriteLine($"The {MainColor} legs walked around the office");

        }

        public virtual void Kick(MiniFigure minifigure)
        {
            Console.WriteLine($"The legs kicked {minifigure.Name}, the {minifigure.Description}.");
        }
    }
}