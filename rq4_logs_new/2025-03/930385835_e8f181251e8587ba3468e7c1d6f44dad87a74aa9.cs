using PetFamily.Core.Abstractions;
using PetFamily.Core.Dto.VolunteerRequest;
using PetFamily.Core.Extensions;
using PetFamily.Core.Models;
using PetFamily.VolunteerRequests.Application.Abstractions;

namespace PetFamily.VolunteerRequests.Application.Queries.GetRequestsForUser;

public class GetRequestsByUserIdHandler
    : IQueryHandler<PagedList<VolunteerRequestDto>, GetRequestsByUserIdQuery>
{
    private readonly IReadDbContext _readDbContext;

    public GetRequestsByUserIdHandler(IReadDbContext readDbContext)
    {
        _readDbContext = readDbContext;
    }

    public async Task<PagedList<VolunteerRequestDto>> HandleAsync(
        GetRequestsByUserIdQuery query,
        CancellationToken cancellationToken)
    {
        var volunteerRequestsQuery = _readDbContext.VolunteerRequests;

        volunteerRequestsQuery = volunteerRequestsQuery.Where(v =>
            v.UserId == query.UserId);
        
        volunteerRequestsQuery = volunteerRequestsQuery.WhereIf(
            query.Status != null,
            p => (p.Status == query.Status!));

        return await volunteerRequestsQuery.ToPagedList(query.Page, query.PageSize, cancellationToken);
    }
}