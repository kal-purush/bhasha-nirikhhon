using DeliveryApp.Core.Application.UseCases.Commands.AssignOrder;
using MediatR;
using Quartz;

namespace DeliveryApp.Api.Adapters.Jobs;

[DisallowConcurrentExecution]
public class AssignOrdersJob(IMediator mediator) : IJob
{
    public async Task Execute(IJobExecutionContext context)
    {
        await mediator.Send(new AssignOrderCommand());
    }
}