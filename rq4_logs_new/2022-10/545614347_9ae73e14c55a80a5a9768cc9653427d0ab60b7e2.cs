using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TellDontAsk
{
    public class Product_Refactored
    {
        private bool _visibility;

        public Product_Refactored(bool visibility)
        {
            _visibility = visibility;
            IsActive();
        }

        public void IsActive()
        {
            if (_visibility = true)
                Console.WriteLine("Product is visible");
            else
                Console.WriteLine("Product is hidden");
        }
    }
}