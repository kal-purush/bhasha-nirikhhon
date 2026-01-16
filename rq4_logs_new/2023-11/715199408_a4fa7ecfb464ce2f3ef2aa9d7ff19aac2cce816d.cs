using MediatR;

namespace NerdStore.Sales.Application.Events.Handlers;

public class OrderEventHandler :
    INotificationHandler<OrderDraftStartedEvent>,
    INotificationHandler<UpdatedOrderEvent>,
    INotificationHandler<OrderItemAddedEvent>
{
    public Task Handle(OrderDraftStartedEvent notification, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public Task Handle(UpdatedOrderEvent notification, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    public Task Handle(OrderItemAddedEvent notification, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}