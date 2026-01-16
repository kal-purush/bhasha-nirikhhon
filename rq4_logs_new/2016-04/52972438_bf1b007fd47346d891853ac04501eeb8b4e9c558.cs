using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PatternsPractice.Creational
{
    class FactoryPattern
    {
        //public static void Main(string[] args)
        //{
        //    List<Factory> factories = new List<Factory>();
        //    factories.Add(new Factory1());
        //    factories.Add(new Factory2());
        //    foreach (Factory factory in factories)
        //    {
        //        AbstractProduct product = factory.Create();
        //        Console.WriteLine("Created {0}",
        //          product.GetType().Name);
        //    }
        //    Console.ReadKey();
        //}
    }

    abstract class AbstractProduct
    {
    }

    class ProductA : AbstractProduct
    { 
    }

    class ProductB : AbstractProduct
    { 
    }

    abstract class Factory
    { 
        public abstract AbstractProduct Create();
    }

    class Factory1 : Factory
    {
        public override AbstractProduct Create()
        {
            return new ProductA();
        }
    }

    class Factory2 : Factory
    {
        public override AbstractProduct Create()
        {
            return new ProductB();
        }
    }
}