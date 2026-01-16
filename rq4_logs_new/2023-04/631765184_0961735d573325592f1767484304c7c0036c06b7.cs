using OrderAPI.Models;
using OrderAPI.ViewModel.Order;
using Refit;

namespace OrderAPI.Service
{
    public interface IApiCustomerService
    {
        [Get("/Customer/GetBySdt")]
        Task<Customer> GetBySdt(string Sdt);
        //Task<Customer> GetBySdt([Authorize] string token , string Sdt);
        [Post("/Customer/Createt")]
        Task<Customer> Createt(CreateCustomer createCustomer);
    }
}