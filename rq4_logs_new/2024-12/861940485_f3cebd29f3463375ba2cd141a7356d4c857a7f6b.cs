namespace ApiaryManagementSystem.Application.Features.BeeQueens.EventHandlers;

using ApiaryManagementSystem.Domain.Events.BeeQueens;
using MediatR;
using Microsoft.Extensions.Logging;

public sealed class BeeQueenCreatedEventHandler(ILogger<BeeQueenCreatedEventHandler> logger) : INotificationHandler<BeeQueenCreatedEvent>
{
    public Task Handle(BeeQueenCreatedEvent notification, CancellationToken cancellationToken)
    {
        logger.LogInformation("Domain event fired: {DomainEvent}", notification.GetType().Name);

        return Task.CompletedTask;
    }
}