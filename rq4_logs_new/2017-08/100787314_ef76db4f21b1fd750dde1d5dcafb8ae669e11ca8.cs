using CustomerAppBLL.BusinessObjects;
using CustomerAppDAL.Entities;

namespace CustomerAppBLL.Converters
{
    public class CustomerConverter
    {
        /// <summary>
        /// Convert CustomerBO to Customer
        /// </summary>
        /// <param name="customer"></param>
        /// <returns>Customer</returns>
        internal Customer Convert(CustomerBO customer)
        {
            return new Customer()
            {
                Id = customer.Id,
                FirstName = customer.FirstName,
                LastName = customer.LastName,
                Address = customer.Address
            };
        }

        /// <summary>
        /// Convert Customer to CustomerBO
        /// </summary>
        /// <param name="customer"></param>
        /// <returns>CustomerBO</returns>
        internal CustomerBO Convert(Customer customer)
        {
            return new CustomerBO()
            {
                Id = customer.Id,
                FirstName = customer.FirstName,
                LastName = customer.LastName,
                Address = customer.Address
            };
        }
    }
}