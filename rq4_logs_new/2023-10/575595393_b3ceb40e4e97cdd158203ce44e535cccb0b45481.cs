using EssentialFrame.Domain.Persistence.Models;

namespace EssentialFrame.Domain.EventSourcing.Persistence.Aggregates.Models;

public record EventSourcingAggregateWithEventsModel
{
    public EventSourcingAggregateWithEventsModel(EventSourcingAggregateDataModel eventSourcingAggregateDataModel,
        IReadOnlyCollection<DomainEventDataModel> domainEventDataModels)
    {
        EventSourcingAggregateDataModel = eventSourcingAggregateDataModel;
        DomainEventDataModels = domainEventDataModels;
    }

    public EventSourcingAggregateDataModel EventSourcingAggregateDataModel { get; }

    public IReadOnlyCollection<DomainEventDataModel> DomainEventDataModels { get; }
}