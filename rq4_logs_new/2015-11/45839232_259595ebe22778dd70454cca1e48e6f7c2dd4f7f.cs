using System;
using Elders.Cronus.DomainModeling;
using Elders.Cronus.Userfull;

namespace Elders.Cronus.AtomicAction.Redis.RevisionStore
{
    /// <summary>
    /// Provides storage for aggregate revisions.
    /// </summary>
    public interface IRevisionStore : IDisposable
    {
        Result<bool> HasRevision(IAggregateRootId aggregateRootId);

        Result<int> GetRevision(IAggregateRootId aggregateRootId);

        Result<bool> SaveRevision(IAggregateRootId aggregateRootId, int revision);

        Result<bool> SaveRevision(IAggregateRootId aggregateRootId, int revision, TimeSpan? expiry);
    }
}