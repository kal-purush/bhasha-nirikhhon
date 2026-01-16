using System.Collections.Generic;
using CustomerAppEntity;

namespace CustomerAppDAL.Context
{
    public class MockContext
    {
        public readonly List<Customer> Customers = new List<Customer>
        {
            new Customer
            {
                Id = 1,
                FirstName = "Bob",
                LastName = "Dylan",
                Address = "BongoStreet 202"
            },

            new Customer
            {
                Id = 2,
                FirstName = "Lars",
                LastName = "Bilde",
                Address = "Ostestrasse 202"
            }
        };
    }
}