using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace VSEat.Models
{
    public class Dish_Order
    {
        public int order_Id { get; set; }
        public string status { get; set; }
        public DateTime created_at { get; set; }
        public DateTime delivery_time { get; set; }
        public int customer_Id { get; set; }
        public string restaurant { get; set; }
        public float totalPrice { get; set; }
        public int dish_Id { get; set; }
        public int quantity { get; set; }
        public float price { get; set; }
        public int delivery_staff_Id { get; set; }
    }
}