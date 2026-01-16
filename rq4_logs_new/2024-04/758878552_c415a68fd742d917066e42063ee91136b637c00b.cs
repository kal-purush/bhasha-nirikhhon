using Jared.Application.Dtos.ProjectDtos;
using Jared.Application.Dtos.TaskDtos;
using Jared.Domain.Abstractions;
using Jared.Domain.Interfaces;
using Jared.Domain.Models;
using MapsterMapper;
using MediatR;
using Microsoft.EntityFrameworkCore;

namespace Jared.Application.Queries.ProjectQueries;

public class ProjectDetailsQueryHandler : IRequestHandler<ProjectDetailsQuery, Result<ProjectDetailsDto>>
{
    private readonly IDataContext dataContext;
    private readonly IMapper mapper;

    public ProjectDetailsQueryHandler(IDataContext dataContext, IMapper mapper)
    {
        this.dataContext = dataContext;
        this.mapper = mapper;
    }

    public async Task<Result<ProjectDetailsDto>> Handle(ProjectDetailsQuery query, CancellationToken cancellationToken)
    {
        try
        {
            var project = await dataContext.Set<Project>()
                .FirstAsync(x => x.Id == query.id, cancellationToken);

            var result = mapper.Map<ProjectDetailsDto>(project);

            return Result.Ok(result);
        }
        catch (Exception ex)
        {
            return new(false, ex.Message);
        }
    }
}