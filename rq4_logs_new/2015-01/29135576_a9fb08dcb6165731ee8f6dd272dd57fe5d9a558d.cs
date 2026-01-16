using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimpleDecoratorPattern
{
    class BasePizza : IPizzaComponent
    {


        public int CalculatePrice()
        {
            return 40;
        }


        public override string ToString()
        {
            return "Pizza with";
        }
    }
}