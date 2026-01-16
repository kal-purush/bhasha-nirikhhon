using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Akka.Actor;

namespace sample_akka_persistence
{
    class Program
    {
        static void Main(string[] args)
        {
            ActorSystem system = ActorSystem.Create("TestSystem");

            var actor = system.ActorOf(Props.Create<SamplePersistenceActor>(() => new SamplePersistenceActor("hello")),"Zinga");


            actor.Tell(new Command("A"));
            actor.Tell(new Command("B"));

            actor.Tell("snap");
            actor.Tell("print");

            Console.Read();
        }
    }
}