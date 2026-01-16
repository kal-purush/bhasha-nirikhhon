namespace ApiaryManagementSystem.Application.Features.BeeQueens.Queries.GetBeeQueens;

using System.Threading;
using System.Threading.Tasks;
using ApiaryManagementSystem.Application.Common.Interfaces;
using ApiaryManagementSystem.Application.Common.Mappings;
using ApiaryManagementSystem.Application.Common.Models;
using AutoMapper;
using AutoMapper.QueryableExtensions;
using MediatR;

using static Common.Constants.ApplicationConstants.Pagination;

public sealed class GetBeeQueensQuery : IRequest<PaginatedList<BeeQueenModel>>
{
    public int? Page { get; init; }

    public int? PageSize { get; init; }
}

internal sealed class GetBeeQueensQueryHandler(
    IApplicationDbContext dbContext,
    IMapper mapper) : IRequestHandler<GetBeeQueensQuery, PaginatedList<BeeQueenModel>>
{
    public async Task<PaginatedList<BeeQueenModel>> Handle(GetBeeQueensQuery request, CancellationToken cancellationToken)
    {
        var page = request.Page ?? DefaultPage;
        var pageSize = request.PageSize ?? DefaultPageSize;

        return await dbContext.BeeQueens
            .ProjectTo<BeeQueenModel>(mapper.ConfigurationProvider)
            .ToPaginatedListAsync(page, pageSize);
    }
}