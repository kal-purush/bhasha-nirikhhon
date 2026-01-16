using CSharpFunctionalExtensions;
using Microsoft.EntityFrameworkCore;
using PetFamily.Application.Database;
using PetFamily.Application.Dto.Volunteer;
using PetFamily.Domain.Shared.CustomErrors;

namespace PetFamily.Application.Volunteers.Queries.GetVolunteerById;

public class GetVolunteerByIdHandler
{
    private readonly IReadDbContext _readDbContext;

    public GetVolunteerByIdHandler(IReadDbContext readDbContext)
    {
        _readDbContext = readDbContext;
    }

    public async Task<Result<VolunteerDto, ErrorList>> HandlerAsync(
        GetVolunteerByIdQuery query,
        CancellationToken cancellationToken)
    {
        var result = await _readDbContext.Volunteers
            .FirstOrDefaultAsync(v => v.Id == query.Id, cancellationToken);

        if (result == null)
        {
            return Errors.General.ValueNotFound(query.Id).ToErrorList();
        }
        
        return result;
    }
}