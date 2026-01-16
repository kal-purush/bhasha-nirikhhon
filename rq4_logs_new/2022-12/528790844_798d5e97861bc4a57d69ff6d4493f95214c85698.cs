using CloudFabric.EventSourcing.EventStore;

namespace CloudFabric.EventSourcing.Domain;

public class AggregateRepositoryFactory
{
    private readonly IEventStore _eventStore;
    
    public AggregateRepositoryFactory(IEventStore eventStore)
    {
        _eventStore = eventStore;
    }

    public AggregateRepository<TAggregate> GetAggregateRepository<TAggregate>() where TAggregate : AggregateBase
    {
        var repository = Activator.CreateInstance(typeof(AggregateRepository<TAggregate>), _eventStore);

        if (repository == null)
        {
            throw new ArgumentException($"Could not create AggregateRepository<${typeof(TAggregate).FullName}>");
        }

        return (AggregateRepository<TAggregate>)repository;
    }
}