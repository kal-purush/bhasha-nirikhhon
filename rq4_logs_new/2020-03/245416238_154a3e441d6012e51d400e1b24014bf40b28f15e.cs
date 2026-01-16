using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ObjectOnDemand_LazyInitialization
{
    /*The Lazy Initialization technique consists of checking the value of a class' field when 
     it's being accessed: if that value equals null, the field gets loaded with the proper 
      value before it is returned. */
    public class Customer
    {
        public int CustomerID { get; set; }
        public string Name { get; set; }
        private IEnumerable<Order> _orders;

        public IEnumerable<Order> Orders
        {
            get
            {
                if (_orders == null)
                {
                    _orders = OrdersDatabase.GetOrders(CustomerID);
                }
                return _orders;
            }
        }

        // Constructor
        public Customer(int id, string name)
        {
            CustomerID = id;
            Name = name;
        }
    }
}