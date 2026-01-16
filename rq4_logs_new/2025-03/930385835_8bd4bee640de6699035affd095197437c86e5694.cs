using CSharpFunctionalExtensions;
using FluentValidation;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using PetFamily.Core.Abstractions;
using PetFamily.Core.Extensions;
using PetFamily.SharedKernel.CustomErrors;
using PetFamily.SharedKernel.Structs;
using PetFamily.SharedKernel.ValueObjects.Ids;
using PetFamily.VolunteerRequests.Application.Abstractions;
using PetFamily.VolunteerRequests.Domain.Entities;
using PetFamily.VolunteerRequests.Domain.ValueObjects;

namespace PetFamily.VolunteerRequests.Application.Commands.CreateVolunteerRequest;



public class CreateVolunteerRequestHandler : ICommandHandler<Guid, CreateVolunteerRequestCommand>
{
    private readonly IVolunteerRequestsRepository _volunteerRequestsRepository;
    private readonly ILogger<CreateVolunteerRequestHandler> _logger;
    private readonly IValidator<CreateVolunteerRequestCommand> _validator;
    private readonly IUnitOfWork _unitOfWork;

    public CreateVolunteerRequestHandler(
        IVolunteerRequestsRepository volunteerRequestsRepository,
        ILogger<CreateVolunteerRequestHandler> logger,
        IValidator<CreateVolunteerRequestCommand> validator,
        [FromKeyedServices(UnitOfWorkSelector.VolunteerRequests)] IUnitOfWork unitOfWork)
    {
        _volunteerRequestsRepository = volunteerRequestsRepository;
        _logger = logger;
        _validator = validator;
        _unitOfWork = unitOfWork;
    }

    public async Task<Result<Guid, ErrorList>> HandleAsync(
        CreateVolunteerRequestCommand command,
        CancellationToken cancellationToken)
    {
        var validationResult = await _validator.ValidateAsync(command, cancellationToken);
        if (validationResult.IsValid == false)
            return validationResult.ToErrorList();
        
        var userId = UserId.Create(command.UserId);
        
        var existedRequest = await _volunteerRequestsRepository
            .GetByUserIdAsync(userId, cancellationToken);
        
        if (existedRequest.IsSuccess)
            return Errors.General.AlreadyExist($"Volunteer request for user with Id = {userId.Value}").ToErrorList();

        var volunteerInfo = VolunteerInfo.Create(command.VolunteerInfo).Value;
        
        var volunteerRequest = new VolunteerRequest(userId, volunteerInfo);
        
        await _volunteerRequestsRepository.AddAsync(volunteerRequest, cancellationToken);
        await _unitOfWork.SaveChangesAsync(cancellationToken);

        _logger.LogInformation(
            "Created VolunteerRequest with ID = {id}", volunteerRequest.Id.Value);
        
        return volunteerRequest.Id.Value;
    }
}