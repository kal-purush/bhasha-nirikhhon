using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PatternsPractice.Structural
{
    class BridgePattern
    {
        //public static void Main(string[] args)
        //{
        //    Abstraction a = new RefinedAbstraction();
        //    a._implementor = new ImplementationA();
        //    a.Execute();
        //    a._implementor = new ImplementationB();
        //    a.Execute();
        //    Console.ReadKey();
        //}
    }

    abstract class Abstraction
    {
        public Implementor _implementor;

        public Implementor Implementor
        {
            set {
                _implementor = value;
            }
        }

        public virtual void Execute()
        {
            if (_implementor != null)
            {
                _implementor.Operation();
            }
        }
    }

    class RefinedAbstraction : Abstraction
    {
        public override void Execute()
        {
            base.Execute();
        }
    }

    interface Implementor
    {
        void Operation();
    }

    class ImplementationA : Implementor
    {
        public void Operation()
        {
            Console.WriteLine("From ImplementationA.");
        }
    }

    class ImplementationB : Implementor
    {
        public void Operation()
        {
            Console.WriteLine("From ImplementationB.");
        }
    }
}