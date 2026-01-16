using System.ComponentModel.DataAnnotations;
using System.Collections.Generic;

namespace PizzaClient.Razor.DTOs
{
    public class Order 
    {
        [Required]
        public List<Pizza> Pizzas { get; set; } = new List<Pizza>();
        [Required]
        public string CustomerName { get; set; }
        
        public Order(string customerName)
        {
            CustomerName = customerName;
        }
    }

    public class Pizza
    {
        [Required]
        public string Size { get; set; }
        [Required]
        public string Dough { get; set; }
        [Required]
        public string[] Toppings { get; set; }
        [Required]
        public string BaseSauce { get; set; }
        [Required]
        public double Price { get; set; }
        public Pizza(string size, string dough, string[] toppings, string baseSauce) 
        {
            Size = size;
            Dough = dough;
            Toppings = toppings;
            BaseSauce = baseSauce;
        }
    }
}