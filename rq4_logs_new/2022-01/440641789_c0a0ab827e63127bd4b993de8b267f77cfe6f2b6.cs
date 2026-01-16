using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace PizzaRestaurant.Models
{
    public class Order
    {
        public int OrderID  { get; set; }
        public int ClientID  { get; set; }
        public List<int>PizzaIDS   { get; set; }
        public int RestaurantID   { get; set; }
        public bool Confirmation   { get; set; }
        public DateTime OrderDate  { get; set; }
        public string DeliveryAddress  { get; set; }


    }
}