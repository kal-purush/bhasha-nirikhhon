using DirectoryProject.DirectoryService.Domain;
using DirectoryProject.DirectoryService.Domain.PositionValueObjects;
using DirectoryProject.DirectoryService.Domain.Shared;

namespace DirectoryProject.DirectoryService.Application.Interfaces;

public interface IPositionRepository
{
    Task<Result<Position>> CreateAsync(
        Position entity,
        CancellationToken cancellationToken = default);

    Task<UnitResult> IsNameUniqueAsync(
        PositionName name,
        CancellationToken cancellationToken = default);
}