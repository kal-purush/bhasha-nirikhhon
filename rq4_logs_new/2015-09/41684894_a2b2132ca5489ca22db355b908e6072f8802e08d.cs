using System;
using System.Reactive.Concurrency;
using System.Threading;
using System.Threading.Tasks;
using DealCapture.Client.Repositories.Dto;
using DealCapture.Client.Repositories.EventStore;
using DealCapture.Client.Repositories.MessageBus;
using EventStore.ClientAPI;

namespace DealCapture.Client.Repositories.Server
{
    //Just a throw together InMemory-InProcess handler.
    class DealEntryCommandHandler : IDisposable
    {
        private readonly IMessageBusClient _messageBusClient;
        private readonly IEventStoreClient _eventStoreClient;
        private EventLoopScheduler _els;

        public DealEntryCommandHandler(IMessageBusClient messageBusClient, IEventStoreClient eventStoreClient)
        {
            _messageBusClient = messageBusClient;
            _eventStoreClient = eventStoreClient;
        }

        public void Start()
        {
            _els = new EventLoopScheduler(ts => new Thread(ts) { IsBackground = true, Name = "DealEntryCommandHandler" });
            
            _els.Schedule(ProcessLoop);
        }

        private void ProcessLoop(Action self)
        {
            ProcessNext().Wait();
            self();
        }

        private async Task ProcessNext()
        {
            //TODO: Allow cancellation
            var cmd = await _messageBusClient.Dequeue<CreateDealCommand>();

            //Run slow just for fun
            await Task.Delay(TimeSpan.FromSeconds(2));

            //Create the DealCreatedEvent in the new Deal-GUID stream.

            var streamName = StreamNames.DealStreamName(cmd.DealId);
            var eventId = Guid.NewGuid();   //Is this correct? Or should it be cmd.DealId? Or something else?
            var payload = cmd.ToJson();
            await _eventStoreClient.SaveEvent(streamName, ExpectedVersion.NoStream, eventId, "DealCreated", payload);

            //We will use Projections to merge them together.
        }

        public void Dispose()
        {
            _els.Dispose();
        }
    }
}