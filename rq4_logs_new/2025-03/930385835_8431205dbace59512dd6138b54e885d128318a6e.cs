using CSharpFunctionalExtensions;
using PetFamily.Core.CustomErrors;
using PetFamily.Species.Domain.ValueObjects.SpeciesVO;

namespace PetFamily.Species.Application.SpeciesManagement;

public interface ISpeciesRepository
{
    Task<Guid> AddAsync(Domain.Entities.Species species, CancellationToken cancellationToken = default);

    Guid Save(Domain.Entities.Species species, CancellationToken cancellationToken = default);

    Guid Delete(Domain.Entities.Species species, CancellationToken cancellationToken = default);
    Task<Result<Domain.Entities.Species, Error>> GetByIdAsync(SpeciesId id, CancellationToken cancellationToken = default);
    
    Task<Result<Domain.Entities.Species, Error>> GetByNameAsync(string speciesName, CancellationToken cancellationToken = default);
}