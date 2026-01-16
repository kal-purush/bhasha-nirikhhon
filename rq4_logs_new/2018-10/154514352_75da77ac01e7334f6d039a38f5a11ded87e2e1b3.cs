using System;
using System.Collections.Generic;
using System.Text;

namespace Crud
{
    class Product
    {
        public int ProductID { get; set; }
        public string Name { get; set; }
        public decimal Price { get; set; }
        public int CategoryID { get; set; }
    }
}