using backend_carhelp.Iam.Domain.Model.Commands;
using backend_carhelp.Iam.Domain.Model.Entities;
using backend_carhelp.Iam.Domain.Repositories;
using backend_carhelp.Iam.Domain.Services;
using backend_carhelp.shared.Domain.Repositories;

namespace backend_carhelp.Iam.Application.Internal.CommandServices;

public class WorkshopCommandService : IWorkshopCommandService
{
    private readonly IWorkshopRepository _workshopRepository;
    private readonly IUnitOfWork _unitOfWork;
    private readonly IUserRepository _userRepository;  
    private IWorkshopCommandService _workshopCommandServciceImplementation;
    
    public WorkshopCommandService(IWorkshopRepository workshopRepository, IUnitOfWork unitOfWork, IUserRepository userRepository)
    {
        _workshopRepository = workshopRepository;
        _unitOfWork = unitOfWork;
        _userRepository = userRepository;
    }

    public async Task<Workshop?> Handle(CreateWorkshopCommand command)
    {
        var user = await _userRepository.GetByIdAsync(command.UserId);
        if (user == null)
        {
            Console.WriteLine($"User with Id {command.UserId} does not exist.");
            return null;
        }

        var workshop = new Workshop()
        {
            UserId = command.UserId
            
        };
        try
        {
            await _workshopRepository.AddAsync(workshop);
            await _unitOfWork.CompleteAsync();
            return workshop;
        }
        catch (Exception e)
        {
            Console.WriteLine($"An error occurred while creating the Customer: {e.Message}");
            return null;
        }
    }

    public async Task Handle(DeleteWorkshopCommand command)
    {
        try
        {
            await _workshopRepository.DeleteWorkshopByIdAsync(command.Id);
          
            await _unitOfWork.CompleteAsync();
        }
        catch (Exception e)
        {
            Console.WriteLine($"An error occurred while deleting the user: {e.Message}");
        }
    }
    
}