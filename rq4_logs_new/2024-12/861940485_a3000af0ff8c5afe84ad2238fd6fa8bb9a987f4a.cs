namespace ApiaryManagementSystem.Application.Features.Diseases.EventHandlers;

using ApiaryManagementSystem.Domain.Events.Diseases;
using MediatR;
using Microsoft.Extensions.Logging;

public sealed class DiseaseCreatedEventHandler(ILogger<DiseaseCreatedEventHandler> logger) : INotificationHandler<DiseaseCreatedEvent>
{
    public Task Handle(DiseaseCreatedEvent notification, CancellationToken cancellationToken)
    {
        logger.LogInformation("Domain event fired: {DomainEvent}", notification.GetType().Name);

        return Task.CompletedTask;
    }
}