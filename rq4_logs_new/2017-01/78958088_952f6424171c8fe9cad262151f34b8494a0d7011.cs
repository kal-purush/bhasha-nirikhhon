using System;

namespace Zoolandia.Animals
{
    public class Platynota : Animal
    {

        public bool IsBug { get; set; }

        public int numberOfWings { get; set; }
        public string noWings { get; set; }

        public class Reptilia : Platynota
        {
            
        }

        public class Flavedana : Platynota 
        {
            public Flavedana (int numberOfWings, string noWings)
            {
                this.numberOfWings = 2;
                this.noWings = "false";
            }
        }
    }
}