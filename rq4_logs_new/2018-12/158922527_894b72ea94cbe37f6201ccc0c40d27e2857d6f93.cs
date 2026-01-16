using System.Collections.Generic;
using AbstractFactoryPattern.Products;

namespace AbstractFactoryPattern.Factories
{
    public abstract class AbstractFactory
    {
        protected abstract AbstractProductA GetProductA(string name);
        protected abstract AbstractProductB GetProductB(string name);

        public List<AbstractProduct> GetProducts(string aName, string bName)
        {
            return new List<AbstractProduct>
            {
                GetProductA(aName),
                GetProductB(bName)
            };
        }
    }
}