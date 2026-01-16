using System;
using System.Collections.Generic;
using System.Text;
using CustomerAppEntity;

namespace CustomerAppBLL
{
    public interface ICustomerService
    {
        /// <summary>
        /// Create a customer
        /// </summary>
        /// <param name="customerToCreate"></param>
        /// <returns></returns>
        Customer CreateCustomer(Customer customerToCreate);

        /// <summary>
        /// Get all customers
        /// </summary>
        /// <returns>Collection of customers</returns>
        IEnumerable<Customer> GetAllCustomers();
        Customer GetCustomerById(int id);

        /// <summary>
        /// Update parsed customer
        /// </summary>
        /// <param name="updatedCustomer"></param>
        /// <returns></returns>
        Customer UpdateCustomer(Customer updatedCustomer);

        /// <summary>
        /// Delete customer by id
        /// </summary>
        /// <param name="id"></param>
        /// <returns>true if deletion succeeded, false otherwise</returns>
        bool DeleteCustomer(int id);
    }
}