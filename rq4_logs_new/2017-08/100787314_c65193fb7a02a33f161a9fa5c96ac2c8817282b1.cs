using System;
using System.Collections.Generic;
using System.Linq;
using CustomerAppEntity;

namespace CustomerAppDAL
{
    public interface ICustomerRepository
    {/// <summary>
        /// Create a customer
        /// </summary>
        /// <param name="customerToCreate"></param>
        /// <returns>Created customer</returns>
        Customer CreateCustomer(Customer customerToCreate);

        /// <summary>
        /// Get all customers
        /// </summary>
        /// <returns>Collection of customers</returns>
        IEnumerable<Customer> GetAllCustomers();
        Customer GetCustomerById(int id);

        /// <summary>
        /// Delete customer by id
        /// </summary>
        /// <param name="id"></param>
        /// <returns>true if deletion succeeded, false otherwise</returns>
        bool DeleteCustomer(int id);
    }
}