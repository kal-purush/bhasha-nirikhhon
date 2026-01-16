using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Delegates
{
    class Delegates
    {
        delegate void GreetingDelegate(string name);

        static void Main(string[] args)
        {
            GreetingDelegate firstDelegate = new GreetingDelegate(Hello);
            GreetingDelegate secondDelegate = new GreetingDelegate(Goodbye);
            firstDelegate += secondDelegate;
            GreetingMethod(firstDelegate, "Suzie");
            GreetingMethod(secondDelegate, "Eli");

        }

        static void Hello(string name)
        {
            Console.WriteLine("Hello {0}", name);
        }

        static void Goodbye(string name)
        {
            Console.WriteLine("Goodbye {0}", name);
        }

        static void GreetingMethod(GreetingDelegate gd, string name)
        {
            Console.WriteLine("The greeting is: ");
            gd(name);
        }
    }
}