using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Day9
{
    class Program
    {
        static void Main(string[] args)
        {
            // 1 standard technique for initializing
            Order order1 = new Order();
            order1.OrderID = 122;
            order1.OrderDate = DateTime.Now;

            // 2 using overloaded contructor
            Order order2 = new Order(123, DateTime.Now);

            // 3 Array initializer;
            int[] numbers = { 1, 2, 3, 4 };
            string[] states = { "ca", "co" };

            // 4 C# 3 Object initializer - implicitly calls the default constructor
            Order order3 = new Order { OrderID = 124, OrderDate = DateTime.Now, OrderAmount = 122.00M };

            // 5 this EXPLICITLY calls the default constructor
            Order order4 = new Order() { OrderID = 124, OrderDate = DateTime.Now, OrderAmount = 122.00M };

            // Nested objcet initializers
            Order order5 = new Order()
            {
                BillingAddress = new Address { Address1 = "123 way", City="vista",State="CA", Zip="92120"},
                ShippingAddress = new Address { Address1 = "1234 way", City = "san diego", State = "CA", Zip = "92120" },
            };

            // 6 Collection initializers - works with any that implements ICollection<T>
            Order order6 = new Order()
            {
                OrderItems = new List<OrderItem>
                {
                    new OrderItem {OrderItemId = 1,ProductName="Ball", Quantity=1},
                    new OrderItem {OrderItemId = 2,ProductName="cup", Quantity=3},
                    new OrderItem {OrderItemId = 3,ProductName="fork", Quantity=4},
                }
            };

            // 7 works with IEnumerable
            List<int> newNumbers = new List<int> { 6, 7, 8, 9 };

            System.Collections.ArrayList orderArray = new System.Collections.ArrayList
            {
                order1,order2,order4,order4,order5
            };

            Console.WriteLine(order6.OrderItems[0].ProductName);
            Console.ReadLine();
        }

        class Order
        {
            public int OrderID { get; set; }
            public DateTime OrderDate { get; set; }
            public decimal OrderAmount { get; set; }
            public List<OrderItem> OrderItems { get; set; }
            public Address BillingAddress { get; set; }
            public Address ShippingAddress { get; set; }

            //default constructor
            public Order()
            {

            }
            //override constructor
            public Order(int orderID, DateTime orderDate)
            {
                OrderID = orderID;
                OrderDate = orderDate;
            }
        }
        class OrderItem
        {
            public int OrderItemId { get; set; }
            public string ProductName { get; set; }
            public int Quantity { get; set; }
        }
        class Address
        {
            public string Address1 { get; set; }
            public string City { get; set; }
            public string State { get; set; }
            public string Zip { get; set; }
        }
        class Customer
        {
            public int CustomerID { get; set; }
            public string Name { get; set; }
        }
        
    }
}