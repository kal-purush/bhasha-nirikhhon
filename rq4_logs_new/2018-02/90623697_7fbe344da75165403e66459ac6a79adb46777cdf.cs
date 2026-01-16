using System;
using EventStore.ClientAPI;

namespace Webjobs.Extensions.Eventstore
{
    public interface IEventFilter
    {
        IObservable<ResolvedEvent> Filter(IObservable<ResolvedEvent> eventStreamObservable);
    }
}