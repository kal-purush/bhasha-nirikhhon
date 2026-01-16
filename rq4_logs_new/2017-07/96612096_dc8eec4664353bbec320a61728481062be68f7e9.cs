using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PharmacyTest
{
    class Program
    {
        static void Main(string[] args)
        {
            var context = new testEntityEntities();
            var cust = new customer()
            {
                cust_id = 3,
                cust_address = "Address test"
            };
            context.customers.Add(cust);
            context.SaveChanges();
        }
    }
}