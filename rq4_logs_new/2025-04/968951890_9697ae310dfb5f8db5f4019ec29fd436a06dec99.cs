using Lucky7_Inventory_System_Application.Constants;
using Lucky7_Inventory_System_Application.Interfaces;
using Lucky7_Inventory_System_Application.Services;
using Lucky7_Inventory_System_Domain.Entities;
using MediatR;
using static Lucky7_Inventory_System_Application.Responses.ServiceResponses;

namespace Lucky7_Inventory_System_Application.Commands.UserCommands.Handlers;

public class CreateUserCommandHandler : IRequestHandler<CreateUserCommand, GetResponse>
{
    private readonly IGenericRepository<User> _repository;

    public CreateUserCommandHandler(IGenericRepository<User> repository)
    {
        _repository = repository;
    }

    public async Task<GetResponse> Handle(CreateUserCommand request, CancellationToken cancellationToken)
    {
        try
        {
            var validationResults = ValidationService.ValidateUser(request.RoleId, request.StatusId);

            if (validationResults.Any())
            {
                return validationResults.First();
            }

            var newUser = new User
            {
                Firstname = request.Firstname,
                Lastname = request.Lastname,
                Password = request.Password,
                RoleId = request.RoleId,
                StatusId = request.StatusId
            };

            var result = await _repository.Add(newUser);

            return new GetResponse(true, result, "User created successfully", StatusResponse.success);
        }
        catch (Exception ex)
        {
            return new GetResponse(false, null, ex.Message, StatusResponse.unhandled);
        }
    }
}