using CSharpFunctionalExtensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using PetFamily.Application.Database;
using PetFamily.Domain.Shared.CustomErrors;
using PetFamily.Domain.SpeciesContext.ValueObjects.BreedVO;

namespace PetFamily.Application.Species.Commands.DeleteBreedById;

public class DeleteBreedByIdHandler
{
    private readonly ISpeciesRepository _speciesRepository;
    private readonly IUnitOfWork _unitOfWork;
    private readonly ILogger<DeleteBreedByIdHandler> _logger;
    private readonly IReadDbContext _readDbContext;
    
    public DeleteBreedByIdHandler(
        ISpeciesRepository speciesRepository,
        IUnitOfWork unitOfWork,
        ILogger<DeleteBreedByIdHandler> logger,
        IReadDbContext readDbContext)
    {
        _speciesRepository = speciesRepository;
        _unitOfWork = unitOfWork;
        _logger = logger;
        _readDbContext = readDbContext;
    }

    public async Task<Result<Guid, ErrorList>> HandleAsync(
        DeleteBreedByIdCommand command,
        CancellationToken cancellationToken)
    {
        var isUsed = await _readDbContext.Pets.
            FirstOrDefaultAsync(p => p.BreedId == command.BreedId, cancellationToken);
        if (isUsed != null)
            return Errors.General.ValueIsRequired(command.BreedId.ToString()).ToErrorList();

        var speciesResult = await _speciesRepository.GetByIdAsync(command.SpeciesId, cancellationToken);
        if (speciesResult.IsFailure)
            return Errors.General.ValueNotFound().ToErrorList();
        
        
        speciesResult.Value.RemoveBreedById(command.BreedId);
        await _unitOfWork.SaveChangesAsync(cancellationToken);
        
        _logger.LogInformation("Successfully deleted breed with ID = {ID}", command.BreedId);

        return command.BreedId;
    }

}