namespace ApiaryManagementSystem.Application.Features.Harvests.EventHandlers;

using ApiaryManagementSystem.Domain.Events.Harvests;
using MediatR;
using Microsoft.Extensions.Logging;

public sealed class HarvestCreatedEventHandler(ILogger<HarvestCreatedEventHandler> logger) : INotificationHandler<HarvestsCreatedEvent>
{
    public Task Handle(HarvestsCreatedEvent notification, CancellationToken cancellationToken)
    {
        logger.LogInformation("Domain event fired: {DomainEvent}", notification.GetType().Name);

        return Task.CompletedTask;
    }
}