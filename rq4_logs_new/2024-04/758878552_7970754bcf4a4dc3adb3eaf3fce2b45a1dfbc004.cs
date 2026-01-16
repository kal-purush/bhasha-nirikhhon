using Jared.Application.Dtos.EpicDtos;
using Jared.Domain.Abstractions;
using Jared.Domain.Interfaces;
using Jared.Domain.Models;
using MapsterMapper;
using MediatR;
using Microsoft.EntityFrameworkCore;

namespace Jared.Application.Queries.EpicQueries;

public class EpicListQueryHandler
    : IRequestHandler<EpicListQuery, Result<List<EpicListDto>>>
{
    private readonly IDataContext dataContext;
    private readonly IMapper mapper;

    public EpicListQueryHandler(IDataContext dataContext, IMapper mapper)
    {
        this.dataContext = dataContext;
        this.mapper = mapper;
    }

    public async Task<Result<List<EpicListDto>>> Handle(EpicListQuery query, CancellationToken cancellationToken)
    {
        var epicsQuery = dataContext
            .Set<Epic>()
            .Where(x => query.projectId == null || query.projectId == 0 || x.ProjectId == query.projectId)
            .AsNoTracking();

        var epics = await epicsQuery.ToListAsync();

        var result = mapper.Map<List<EpicListDto>>(epics);

        return Result.Ok(result);
    }
}