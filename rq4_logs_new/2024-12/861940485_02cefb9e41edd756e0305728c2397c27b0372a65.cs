namespace ApiaryManagementSystem.Application.Features.Harvests.Queries.GetHarvests;

using System.Threading;
using System.Threading.Tasks;
using ApiaryManagementSystem.Application.Common.Interfaces;
using ApiaryManagementSystem.Application.Common.Mappings;
using ApiaryManagementSystem.Application.Common.Models;
using AutoMapper;
using AutoMapper.QueryableExtensions;
using MediatR;

using static Common.Constants.ApplicationConstants.Pagination;

public sealed class GetHarvestsQuery : IRequest<PaginatedList<HarvestModel>>
{
    public int? Page { get; init; }

    public int? PageSize { get; init; }
}

internal sealed class GetHarvestsQueryHandler(
    IApplicationDbContext dbContext,
    IMapper mapper) : IRequestHandler<GetHarvestsQuery, PaginatedList<HarvestModel>>
{
    public async Task<PaginatedList<HarvestModel>> Handle(GetHarvestsQuery request, CancellationToken cancellationToken)
    {
        var page = request.Page ?? DefaultPage;
        var pageSize = request.PageSize ?? DefaultPageSize;

        return await dbContext.Harvests
            .ProjectTo<HarvestModel>(mapper.ConfigurationProvider)
            .ToPaginatedListAsync(page, pageSize);
    }
}