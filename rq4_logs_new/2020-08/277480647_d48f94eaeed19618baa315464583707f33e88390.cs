using Freddy.Application.Core.Commands;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Freddy.Application.Commands.Customers.AddCustomer
{
    public class AddCustomer : IHandleCommands<AddCustomerCommand>
    {
        private readonly ICustomers _customers;

        public AddCustomer(ICustomers customers)
        {
            _customers = customers;
        }

        public Task Handle(AddCustomerCommand command)
        {
            return _customers.Add(new Customer(command.CustomerId, command.CustomerInfo));
        }
    }
}