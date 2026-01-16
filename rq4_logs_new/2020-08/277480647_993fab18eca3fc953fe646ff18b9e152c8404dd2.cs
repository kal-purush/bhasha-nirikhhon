using Freddy.Application.Core.Queries;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Freddy.Application.Queries.Customers.GetCustomerById
{
    public class GetCustomerById : IExecuteQuery<GetCustomerByIdQuery, CustomerView>
    {
        private readonly ICustomerViews _customers;

        public GetCustomerById(ICustomerViews customers)
        {
            _customers = customers;
        }

        public async Task<CustomerView> Execute(GetCustomerByIdQuery query)
        {
            return await _customers.Customers.FirstOrDefaultAsync(c => c.Id == query.CustomerId);
        }
    }
}