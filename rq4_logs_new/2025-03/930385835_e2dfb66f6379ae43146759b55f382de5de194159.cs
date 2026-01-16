using CSharpFunctionalExtensions;
using PetFamily.SharedKernel.CustomErrors;
using PetFamily.SharedKernel.ValueObjects.Ids;
using PetFamily.VolunteerRequests.Domain.Entities;

namespace PetFamily.VolunteerRequests.Application.Abstractions;


public interface IVolunteerRequestsRepository
{
    Task<Guid> AddAsync(VolunteerRequest volunteerRequest, CancellationToken cancellationToken = default);

    Guid Save(VolunteerRequest volunteerRequest, CancellationToken cancellationToken = default);
    
    Guid Delete(VolunteerRequest volunteerRequest, CancellationToken cancellationToken = default);
    
    Task<Result<VolunteerRequest, Error>> GetByIdAsync(VolunteerRequestId id, CancellationToken cancellationToken = default);
}