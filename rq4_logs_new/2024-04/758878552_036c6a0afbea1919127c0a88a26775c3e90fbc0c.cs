using Jared.Application.Dtos.TaskDtos;
using Jared.Domain.Abstractions;
using Jared.Domain.Interfaces;
using Mapster;
using MapsterMapper;
using MediatR;
using Microsoft.EntityFrameworkCore;

namespace Jared.Application.Commands.TaskCommands;

public class TaskUpdateCommandHandler : IRequestHandler<TaskUpdateCommand, Result<TaskDetailsDto>>
{
    private readonly IDataContext dataContext;
    private readonly IMapper mapper;

    public TaskUpdateCommandHandler(IDataContext dataContext, IMapper mapper)
    {
        this.dataContext = dataContext;
        this.mapper = mapper;
    }

    public async Task<Result<TaskDetailsDto>> Handle(TaskUpdateCommand command, CancellationToken cancellationToken)
    {
        try
        {
            var task = await dataContext.Set<Domain.Models.Task>()
                .Include(x => x.Project)
                .Include(x => x.Epic)
                .FirstAsync(x => x.Id == command.dto.Id);

            task!.Title = command.dto.Title;
            await dataContext.SaveChangesAsync(cancellationToken);

            return Result.Ok(command.dto);
        }
        catch (Exception ex)
        {
            return new(false, ex.Message);

        }

    }
}