using System.Data;
using AutoMapper;
using Maki.Domain.Customer.Models.Commands;
using Maki.Domain.Customer.Models.Response;
using Maki.Domain.Customer.Repositories;
using Maki.Domain.Customer.Services;

namespace Maki.Application.Customer.CommandServices;

public class CustomerCommandService : ICustomerCommandService
{
    private readonly ICustomerRepository _customerRepository;
    private readonly IMapper _mapper;

    public CustomerCommandService(ICustomerRepository customerRepository, IMapper mapper)
    {
        _customerRepository = customerRepository;
        _mapper = mapper;
    }

    public async Task<int> Handle(RegisterCustomerCommand command)
    {
        var customer = _mapper.Map<RegisterCustomerCommand, Domain.Customer.Models.Queries.Customer>(command);
        var existingCustomer = await _customerRepository.GetByEmailAndPasswordAsync(customer.Email, command.Password);
        if (existingCustomer != null) throw new DuplicateNameException("Customer already exists");

        var result = await _customerRepository.RegisterAsync(customer);
        return result.Id;  
    }
    public async Task<CustomerResponse> Handle(LoginCustomerCommand command)
    {
        var customer = await _customerRepository.GetByEmailAndPasswordAsync(command.Email, command.Password);
        if (customer == null) throw new UnauthorizedAccessException("Invalid email or password");

        return _mapper.Map<Domain.Customer.Models.Queries.Customer, CustomerResponse>(customer);
    }

    public async Task<bool> Handle(UpdateCustomerCommand command)
    {
        var existingCustomer = await _customerRepository.GetByIdAsync(command.Id);
        if (existingCustomer == null) throw new KeyNotFoundException("Customer not found");

        var customer = _mapper.Map<UpdateCustomerCommand, Domain.Customer.Models.Queries.Customer>(command);
        return await _customerRepository.UpdateAsync(customer);
    }

    public async Task<bool> Handle(DeleteCustomerCommand command)
    {
        var existingCustomer = await _customerRepository.GetByIdAsync(command.Id);
        if (existingCustomer == null) throw new KeyNotFoundException("Customer not found");

        return await _customerRepository.DeleteAsync(command.Id);
    }
}