using backend_carhelp.Iam.Domain.Model.Aggregates;
using backend_carhelp.Iam.Domain.Model.Commands;
using backend_carhelp.Iam.Domain.Model.Entities;
using backend_carhelp.Iam.Domain.Model.ValueObjects;
using backend_carhelp.Iam.Domain.Repositories;
using backend_carhelp.Iam.Domain.Services;
using backend_carhelp.shared.Domain.Repositories;

public class CustomerCommandService : ICustomerCommandServcice
{
    private readonly ICustomerRepository _customerRepository;
    private readonly IUnitOfWork _unitOfWork;
    private readonly IUserRepository _userRepository;  
    private ICustomerCommandServcice _customerCommandServciceImplementation;

    public CustomerCommandService(ICustomerRepository customerRepository, IUnitOfWork unitOfWork, IUserRepository userRepository)
    {
        _customerRepository = customerRepository;
        _unitOfWork = unitOfWork;
        _userRepository = userRepository;
    }

    public async Task<Customer?> Handle(CreateCustomerCommand command)
    {
        var user = await _userRepository.GetByIdAsync(command.UserId);
        if (user == null)
        {
            Console.WriteLine($"User with Id {command.UserId} does not exist.");
            return null;
        }

        var customer = new Customer
        {
            UserId = command.UserId
            
        };
        try
        {
            await _customerRepository.AddAsync(customer);
            await _unitOfWork.CompleteAsync();
            return customer;
        }
        catch (Exception e)
        {
            Console.WriteLine($"An error occurred while creating the Customer: {e.Message}");
            return null;
        }
    }
    
    public async Task Handle(DeleteCustomerCommand command)
    {
        try
        {
            await _customerRepository.DeleteCustomerByIdAsync(command.Id);
          
            await _unitOfWork.CompleteAsync();
        }
        catch (Exception e)
        {
            Console.WriteLine($"An error occurred while deleting the user: {e.Message}");
        }
    }

    
}