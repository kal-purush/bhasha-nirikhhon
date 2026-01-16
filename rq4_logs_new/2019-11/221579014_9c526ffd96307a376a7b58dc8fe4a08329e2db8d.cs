using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.ComponentModel.DataAnnotations;

namespace ChajdPizzaWebApp.Models
{
    public class OrderDetail
    {
        public int Id { get; set; }
        
        [Required]
        public int OrderId { get; set; }

        [Required]
        public int SizeId { get; set; }

        [Display(Name = "Selected Toppings")]
        public string ToppingsSelected { get; set; }

        [Display(Name = "Number of Toppings")]
        public int ToppingsCount { get; set; }

        [DataType(DataType.Currency)]
        public double Price { get; set; }
        public string SpecialRequest { get; set; }

        //public Order Orders { get; set; }
        //public Size Sizes { get; set; }

    }
}