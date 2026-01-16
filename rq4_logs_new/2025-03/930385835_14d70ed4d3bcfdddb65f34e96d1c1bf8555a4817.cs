using PetFamily.Application.Abstractions;
using PetFamily.Application.Database;
using PetFamily.Application.Dto.Pet;
using PetFamily.Application.Dto.Volunteer;
using PetFamily.Application.Extensions;
using PetFamily.Application.Models;
using PetFamily.Application.Volunteers.Queries.GetVolunteersWithPagination;

namespace PetFamily.Application.Pets.Queries.GetFilteredPetsWithPagination;



public class GetFilteredPetsWithPaginationHandler : IQueryHandler<
    PagedList<PetDto>,
    GetFilteredPetsWithPaginationQuery>
{
    private readonly IReadDbContext _readDbContext;

    public GetFilteredPetsWithPaginationHandler(IReadDbContext readDbContext)
    {
        _readDbContext = readDbContext;
    }

    public async Task<PagedList<PetDto>> HandleAsync(
        GetFilteredPetsWithPaginationQuery query,
        CancellationToken cancellationToken)
    {
        var volunteersQuery = _readDbContext.Pets;
        
        return await volunteersQuery.ToPagedList(query.Page, query.PageSize, cancellationToken);
    }
}