using CSharpFunctionalExtensions;
using FluentValidation;
using Microsoft.Extensions.Logging;
using PetFamily.Application.Database;
using PetFamily.Application.Extensions;
using PetFamily.Domain.Shared.CustomErrors;
using PetFamily.Domain.Shared.SharedVO;
using PetFamily.Domain.SpeciesContext.Entities;
using PetFamily.Domain.SpeciesContext.ValueObjects.BreedVO;
using PetFamily.Domain.SpeciesContext.ValueObjects.SpeciesVO;

namespace PetFamily.Application.Species.Commands.AddBreedToSpecies;

public class AddBreedToSpeciesHandler
{
    private readonly IValidator<AddBreedToSpeciesCommand> _validator;
    private readonly ISpeciesRepository _speciesRepository;
    private readonly ILogger<AddBreedToSpeciesHandler> _logger;
    private readonly IUnitOfWork _unitOfWork;

    public AddBreedToSpeciesHandler(
        IValidator<AddBreedToSpeciesCommand> validator,
        ISpeciesRepository speciesRepository,
        ILogger<AddBreedToSpeciesHandler> logger,
        IUnitOfWork unitOfWork)
    {
        _validator = validator;
        _speciesRepository = speciesRepository;
        _logger = logger;
        _unitOfWork = unitOfWork;
    }

    public async Task<Result<Guid, ErrorList>> HandleAsync(
        AddBreedToSpeciesCommand command,
        CancellationToken cancellationToken = default)
    {
        var validationResult = await _validator.ValidateAsync(command, cancellationToken);
        if (validationResult.IsValid == false)
            return validationResult.ToErrorList();


        var speciesId = SpeciesId.Create(command.Id);
        var speciesResult = await _speciesRepository.GetByIdAsync(speciesId, cancellationToken);
        if (speciesResult.IsFailure)
            return speciesResult.Error.ToErrorList();

        var id = BreedId.NewBreedId();

        var name = Name.Create(command.Name).Value;

        var breed = new Breed(id, name);

        speciesResult.Value.AddBreed(breed);

        await _unitOfWork.SaveChangesAsync(cancellationToken);

        _logger.LogInformation(
            "Successfully added breed with ID = {ID} to species with ID = {ID}", breed.Id.Value, speciesId.Value);

        return breed.Id.Value;
    }
}