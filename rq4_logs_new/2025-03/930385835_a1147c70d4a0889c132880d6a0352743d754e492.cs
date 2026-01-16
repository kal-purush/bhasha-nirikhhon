using CSharpFunctionalExtensions;
using Microsoft.EntityFrameworkCore;
using PetFamily.Core.Abstractions;
using PetFamily.SharedKernel.CustomErrors;

namespace PetFamily.Species.Application.Queries.CheckBreedToSpeciesExistence;



public class CheckBreedToSpeciesExistenceHandler : IQueryHandler<UnitResult<Error>, CheckBreedToSpeciesExistenceQuery>
{
    private readonly IReadDbContext _readDbContext;

    public CheckBreedToSpeciesExistenceHandler(IReadDbContext readDbContext)
    {
        _readDbContext = readDbContext;
    }

    public async Task<UnitResult<Error>> HandleAsync(
        CheckBreedToSpeciesExistenceQuery query,
        CancellationToken cancellationToken)
    {
        var speciesExist = await _readDbContext.Species
            .FirstOrDefaultAsync(s => s.Id == query.SpeciesId, cancellationToken);

        if (speciesExist == null)
            return Errors.General.ValueNotFound(query.SpeciesId);

        var breedExist = await _readDbContext.Breeds
            .FirstOrDefaultAsync(b => b.Id == query.BreedId &&
                                      b.SpeciesId == query.SpeciesId, cancellationToken);

        if (breedExist == null)
            return Errors.General.ValueNotFound(query.BreedId);

        return UnitResult.Success<Error>();
    }
}