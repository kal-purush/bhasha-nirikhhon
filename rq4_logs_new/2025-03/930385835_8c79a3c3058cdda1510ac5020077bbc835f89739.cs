using CSharpFunctionalExtensions;
using Microsoft.EntityFrameworkCore;
using PetFamily.Application.Database;
using PetFamily.Application.Dto.Pet;
using PetFamily.Application.Dto.Volunteer;
using PetFamily.Application.Volunteers.Queries.GetVolunteerById;
using PetFamily.Domain.Shared.CustomErrors;

namespace PetFamily.Application.Pets.Queries;

public class GetPetByIdHandler
{
    private readonly IReadDbContext _readDbContext;

    public GetPetByIdHandler(IReadDbContext readDbContext)
    {
        _readDbContext = readDbContext;
    }

    public async Task<Result<PetDto, ErrorList>> HandlerAsync(
        GetPetByIdQuery query,
        CancellationToken cancellationToken)
    {
        var result = await _readDbContext.Pets
            .FirstOrDefaultAsync(v => v.Id == query.Id, cancellationToken);
        
        if (result == null)
        {
            return Errors.General.ValueNotFound(query.Id).ToErrorList();
        }
        
        result.TransferDetails = result.TransferDetails.OrderBy(t => t.Name).ToArray();

        return result;
    }
}