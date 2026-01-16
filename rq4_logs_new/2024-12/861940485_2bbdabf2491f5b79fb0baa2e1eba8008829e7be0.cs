namespace ApiaryManagementSystem.Application.Features.Diseases.Queries.GetDiseases;

using System.Threading;
using System.Threading.Tasks;
using ApiaryManagementSystem.Application.Common.Interfaces;
using ApiaryManagementSystem.Application.Common.Mappings;
using ApiaryManagementSystem.Application.Common.Models;
using AutoMapper;
using AutoMapper.QueryableExtensions;
using MediatR;

using static Common.Constants.ApplicationConstants.Pagination;

public sealed class GetDiseasesQuery : IRequest<PaginatedList<DiseaseModel>>
{
    public int? Page { get; init; }

    public int? PageSize { get; init; }
}

internal sealed class GetDiseasesQueryHandler(
    IApplicationDbContext dbContext,
    IMapper mapper) : IRequestHandler<GetDiseasesQuery, PaginatedList<DiseaseModel>>
{
    public async Task<PaginatedList<DiseaseModel>> Handle(GetDiseasesQuery request, CancellationToken cancellationToken)
    {
        var page = request.Page ?? DefaultPage;
        var pageSize = request.PageSize ?? DefaultPageSize;

        return await dbContext.Diseases
            .ProjectTo<DiseaseModel>(mapper.ConfigurationProvider)
            .ToPaginatedListAsync(page, pageSize);
    }
}