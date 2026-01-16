using System;

namespace legoMiniFigures.Heads
{
    class BaldHead : Head
    {
        private readonly string _name;

        public override bool HasHair { get { return false; } }
        public int SmoothbessLevel { get; set; }

        public BaldHead(string name)
        {
            _name = name;
        }

        public override void Talk()
        {
            Console.WriteLine($"Hi, I'm {_name}");
        }

        public override void Eat()
        {
            Console.WriteLine("*Eats a burrito*");
        }

    }
}