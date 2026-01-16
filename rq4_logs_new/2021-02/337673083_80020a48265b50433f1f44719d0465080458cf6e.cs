using System;
using System.Collections.Generic;
using System.Text;

namespace Bridge
{
    class Abstraction
    {
        IBridge implemen;
        Dictionary<string, double> products;

        // 1st Method, Constructor receives the concrete implementation
        public Abstraction(IBridge imp, Dictionary<string, double> product)
        {
            implemen = imp;
            products = product;
        }

        //2nd method, just for example purposes

        public Abstraction(int type, Dictionary<string, double> product)
        {
            if (type == 1)
            {
                implemen = new Imple1();
            }
            if (type == 2)
            {
                implemen = new Impl32();
            }/*
            if (type == 3)
            {
                implemen = new Imple3();
            }
            */
            products = product;
        }
    
    // Here, the abstraction communicates with the implementation

        public void ShowTotal()
        {
            implemen.ShowTotal(products);
        }
        public void List()
        {
            implemen.ProductsList(products);
        }
    }
}