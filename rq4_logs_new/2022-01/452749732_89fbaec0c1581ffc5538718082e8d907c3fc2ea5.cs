using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Lesson15.Modules
{
    public class Candy : Product
    {
        public string SugarAmount { get; set; }
        public Candy(string name, int barcode, double weight, double price, string sugarAmount) : base(name, barcode, weight, price)
        {
            SugarAmount = sugarAmount;
        }
        public Candy()
        { }

        public override string ToString()
        {
            return $"{Barcode}, {Name},sugar amount in 100g -{SugarAmount}, weight-{Weight}kg, price - {Price}Eu";
        }
    }
}