using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;

namespace WebWasted.Models
{
    public class Order
    {
        [Key]
        public int ID { get; set; }
        public Food FoodOrder { get; set; }
        public double Amount { get; set; }
    }
}