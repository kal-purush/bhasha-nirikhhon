using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using SPS.AEM.Database.Contexts;
using SPS.AEM.Database.Entities;
using SPS.AEM.Repository.Interfaces;
using SPS.AEM.Repository.Models;

namespace SPS.AEM.Repository.Implementations
{
    public class CustomerRepository : ICustomerRepository
    {
        private readonly AemDatabaseContext context;

        public CustomerRepository(AemDatabaseContext context)
        {
            this.context = context;
        }

        public async Task<IEnumerable<CustomerDto>> GetAllCustomersAsync()
        {
            return await this.context.Customers
                .Select(p => new CustomerDto
                {
                    Id = p.Id,
                    FirstName = p.FirstName,
                    LastName = p.LastName,
                    VillageId = p.VillageId,
                    LoadTypeId = p.LoadTypeId,
                    MobileNumber = p.MobileNumber,
                    RRNumber = p.RRNumber,
                    TransformerId = p.TransformerId
                })
                .ToListAsync();
        }

        public async Task<CustomerDto> GetCustomerByIdAsync(int id)
        {
            return await this.context.Customers
                .Select(p => new CustomerDto
                {
                    Id = p.Id,
                    FirstName = p.FirstName,
                    LastName = p.LastName,
                    VillageId = p.VillageId,
                    LoadTypeId = p.LoadTypeId,
                    MobileNumber = p.MobileNumber,
                    RRNumber = p.RRNumber,
                    TransformerId = p.TransformerId
                })
                .FirstOrDefaultAsync(p => p.Id == id);
        }

        public async Task AddCustomerAsync(CustomerDto customer)
        {
            var customerToBeAdded = new Customer
            {
                Id = customer.Id,
                FirstName = customer.FirstName,
                LastName = customer.LastName,
                VillageId = customer.VillageId,
                LoadTypeId = customer.LoadTypeId,
                MobileNumber = customer.MobileNumber,
                RRNumber = customer.RRNumber,
                TransformerId = customer.TransformerId
            };

            this.context.Customers.Add(customerToBeAdded);
            customer.Id = customerToBeAdded.Id;
            await this.context.SaveChangesAsync();
        }
    }
}