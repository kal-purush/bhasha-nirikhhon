using System;
using Zoolandia.Animals;

namespace Zoolandia
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            var MargaretHamilton = new WeinusCaninus();

            MargaretHamilton.IsSuperLong = "yes";
            MargaretHamilton.name = "Margaret Hamilton";
            MargaretHamilton.type = "weiner dog";
            
            Console.WriteLine($"I have a {MargaretHamilton.type} named {MargaretHamilton.name}. When asked if she was really long, the vet said {MargaretHamilton.IsSuperLong}.");

            var Zebra = new EquusZebra();

            Zebra.IsStriped = "not striped";
            Zebra.name = "Rodney";
            Zebra.color = "orange";

            Console.WriteLine($"I went to the zoo and saw a zebra named {Zebra.name}. Incredibly, he was {Zebra.IsStriped}, and I think he was painted {Zebra.color}.");

            var Lizard = new PlatynotaReptilia();

            Lizard.IsMammal = "not mammals";
            Lizard.name = "Rex";
            Lizard.color = "green";

            Console.WriteLine($"The zoo also had a {Lizard.color} lizard named {Lizard.name}. Lizards in general are {Lizard.IsMammal}.");
        }
    }
}