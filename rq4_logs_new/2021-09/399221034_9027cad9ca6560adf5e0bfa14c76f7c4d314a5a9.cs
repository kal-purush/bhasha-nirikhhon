using System.ComponentModel.DataAnnotations;
using System.Collections.Generic;

namespace PizzaClient.Blazor.DTOs
{
    public class Order 
    {
        [Required(ErrorMessage = "You have not chosen any pizza!")]
        public Pizza Pizza { get; set; }
        [Required(ErrorMessage = "Please enter your name")]
        public string CustomerName { get; set; } 
    }

    public class Pizza
    {
        [Required()]
        public string Size { get; set; }
        [Required()]
        public string Dough { get; set; }
        [Required()]
        public List<string> Toppings { get; set; } = new List<string>();
        [Required()]
        public string BaseSauce { get; set; }
        [Required()]
        public double Price { get; set; }
    }
}