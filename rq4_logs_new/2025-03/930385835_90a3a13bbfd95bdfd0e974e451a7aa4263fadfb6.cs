using CSharpFunctionalExtensions;
using FluentValidation;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using PetFamily.Application.Database;
using PetFamily.Application.Extensions;
using PetFamily.Domain.PetContext.ValueObjects.PetVO;
using PetFamily.Domain.PetContext.ValueObjects.VolunteerVO;
using PetFamily.Domain.Shared.CustomErrors;
using PetFamily.Domain.Shared.SharedVO;

namespace PetFamily.Application.Volunteers.Commands.UpdatePetMainPhoto;

public class UpdatePetMainPhotoHandler
{
    private readonly IValidator<UpdatePetMainPhotoCommand> _validator;
    private readonly IVolunteersRepository _volunteersRepository;
    private readonly ILogger<UpdatePetMainPhotoHandler> _logger;
    private readonly IUnitOfWork _unitOfWork;

    public UpdatePetMainPhotoHandler(
        IValidator<UpdatePetMainPhotoCommand> validator,
        IVolunteersRepository volunteersRepository,
        ILogger<UpdatePetMainPhotoHandler> logger,
        IUnitOfWork unitOfWork)
    {
        _validator = validator;
        _volunteersRepository = volunteersRepository;
        _logger = logger;
        _unitOfWork = unitOfWork;
    }

    public async Task<Result<Guid, ErrorList>> HandleAsync(
        UpdatePetMainPhotoCommand command,
        CancellationToken cancellationToken = default)
    {
        var validationResult = await _validator.ValidateAsync(command, cancellationToken);
        if (validationResult.IsValid == false)
            return validationResult.ToErrorList();


        var volunteerId = VolunteerId.Create(command.VolunteerId);
        var volunteerResult = await _volunteersRepository.GetByIdAsync(volunteerId, cancellationToken);
        if (volunteerResult.IsFailure)
            return volunteerResult.Error.ToErrorList();

        var updateResult = volunteerResult.Value.UpdatePetMainPhoto(command.PetId, command.MainPhotoPath);
        if (updateResult.IsFailure)
            return updateResult.Error.ToErrorList();
        
        await _unitOfWork.SaveChangesAsync(cancellationToken);

        _logger.LogInformation("Successfully updated main photo of pet with ID = {ID}", command.PetId);

        return command.PetId;
    }
}