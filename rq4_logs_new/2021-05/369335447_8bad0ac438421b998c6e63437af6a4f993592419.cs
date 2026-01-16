using System;
namespace ACM.BL.Repositories
{
    public class CustomerRepository
    {
        public Customer Retrieve(int customerId)
        {
            Customer customer = new Customer(customerId);


            // TEMPORARY HARD CODED CUSTOMER DATA
            // Not creating Data Layer for this project
            if (customer.CustomerId == 1)
            {
                customer.Email = "Foo@boo.com";
                customer.FirstName = "Foo";
                customer.LastName = "Bar";
            }
            return customer;
        }
    }
}