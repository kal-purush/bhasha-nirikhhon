using Jared.Domain.Abstractions;
using Jared.Domain.Interfaces;
using Jared.Domain.Models;
using Mapster;
using MediatR;
using Microsoft.EntityFrameworkCore;

namespace Jared.Application.Commands.ProjectCommands;

public class ProjectUpdateCommandHandler : IRequestHandler<ProjectUpdateCommand, Result>
{
    private readonly IDataContext dataContext;

    public ProjectUpdateCommandHandler(IDataContext dataContext)
    {
        this.dataContext = dataContext;
    }

    public async Task<Result> Handle(ProjectUpdateCommand command, CancellationToken cancellationToken)
    {
        try
        {
            var project = await dataContext.Set<Project>()
                .FirstAsync(x => x.Id == command.dto.Id);

            command.dto.Adapt(project);
            await dataContext.SaveChangesAsync(cancellationToken);

            return Result.Ok(command.dto);
        }
        catch (Exception ex)
        {
            return Result.Fail(ex.Message);
        }
    }
}