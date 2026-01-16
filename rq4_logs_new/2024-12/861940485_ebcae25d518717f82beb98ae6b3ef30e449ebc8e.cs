namespace ApiaryManagementSystem.Application.Features.Harvests.Queries.GetHarvestById;

using System.Threading;
using System.Threading.Tasks;
using ApiaryManagementSystem.Application.Common.Interfaces;
using Ardalis.GuardClauses;
using AutoMapper;
using AutoMapper.QueryableExtensions;
using MediatR;
using Microsoft.EntityFrameworkCore;

public sealed class GetHarvestByIdQuery : IRequest<HarvestModel>
{
    public Guid HarvestId { get; init; }
}

internal sealed class GetHarvestByIdQueryHandler(
    IApplicationDbContext dbContext,
    IMapper mapper) : IRequestHandler<GetHarvestByIdQuery, HarvestModel>
{
    public async Task<HarvestModel> Handle(GetHarvestByIdQuery request, CancellationToken cancellationToken)
    {
        var harvest = await dbContext.Harvests
            .Where(x => x.Id == request.HarvestId)
            .AsNoTracking()
            .ProjectTo<HarvestModel>(mapper.ConfigurationProvider)
            .FirstOrDefaultAsync(cancellationToken);

        Guard.Against.NotFound(request.HarvestId, harvest);

        return harvest;
    }
}