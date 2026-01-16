using OnlineStore.Domain.Entities;
using OnlineStore.Service.DTOs.Customer;

namespace OnlineStore.Service.Interfaces
{
    public interface ICustomerService
    {
        Task<Customer> AddCustomerAsync(CustomerForCreationDto customerDto);
        Task<IQueryable<Customer>> RetrieveAllCustomers();
        Task<Customer> RetrieveCustomerByIdAsync(int customerId);
        Task<Customer> ModifyCustomerAsync(int customerId, CustomerForUpdateDTo customerDto);
        Task<bool> RemoveCustomerByIdAsync(int customerId);
    }
}