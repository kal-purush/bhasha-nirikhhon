using CSharpFunctionalExtensions;
using FluentValidation;
using Microsoft.Extensions.Logging;
using PetFamily.Application.Database;
using PetFamily.Application.Extensions;
using PetFamily.Application.Volunteers.Commands.Delete;
using PetFamily.Domain.PetContext.ValueObjects.VolunteerVO;
using PetFamily.Domain.Shared.CustomErrors;

namespace PetFamily.Application.Volunteers.Commands.DeletePet;

public class HardDeletePetHandler
{
    private readonly IVolunteersRepository _volunteersRepository;
    private readonly ILogger<HardDeletePetHandler> _logger;
    private readonly IValidator<DeletePetCommand> _validator;
    private readonly IUnitOfWork _unitOfWork;

    public HardDeletePetHandler(
        IVolunteersRepository volunteersRepository,
        ILogger<HardDeletePetHandler> logger,
        IValidator<DeletePetCommand> validator,
        IUnitOfWork unitOfWork)
    {
        _volunteersRepository = volunteersRepository;
        _logger = logger;
        _validator = validator;
        _unitOfWork = unitOfWork;
    }

    public async Task<Result<Guid, ErrorList>> HandleAsync(
        DeletePetCommand command,
        CancellationToken cancellationToken)
    {
        var validationResult = await _validator.ValidateAsync(command, cancellationToken);
        if (validationResult.IsValid == false)
            return validationResult.ToErrorList();
        
        var volunteerId = VolunteerId.Create(command.VolunteerId);

        var volunteerResult = await _volunteersRepository.GetByIdAsync(volunteerId, cancellationToken);
        if (volunteerResult.IsFailure)
            return volunteerResult.Error.ToErrorList();
        
        var pet = volunteerResult.Value.AllOwnedPets.FirstOrDefault(p => p.Id == command.PetId);
        if (pet == null)
            return Errors.General.ValueNotFound(command.PetId).ToErrorList();

        volunteerResult.Value.HardDeletePet(pet);
        await _unitOfWork.SaveChangesAsync(cancellationToken);
        
        _logger.LogInformation("Pet was hard deleted, his ID = {ID}", pet.Id);

        return pet.Id.Value;
    }
}