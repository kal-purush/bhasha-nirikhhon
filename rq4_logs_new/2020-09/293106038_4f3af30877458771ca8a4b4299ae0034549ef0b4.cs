using BoardAndBarber.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace BoardAndBarber.Data
{
    public class CustomerRepository
    {
        static List<Customer> _customers = new List<Customer>();

        public void Add(Customer customerToAdd)
        {
            _customers.Add(customerToAdd);
        }
    }
}