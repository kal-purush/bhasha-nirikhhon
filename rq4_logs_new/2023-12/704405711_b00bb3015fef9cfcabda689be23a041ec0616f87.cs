using System;
using System.Threading.Tasks;

using Microsoft.Extensions.Configuration;
using Testcontainers.EventStoreDb;

namespace HomeBudget.Accounting.Api.IntegrationTests
{
    internal class TestContainersService(IConfiguration configuration) : IAsyncDisposable
    {
        public EventStoreDbContainer EventSourceDbContainer { get; private set; }

        public async Task UpAndRunningContainersAsync()
        {
            if (configuration == null)
            {
                return;
            }

            EventSourceDbContainer = new EventStoreDbBuilder()
                .WithImage("eventstore/eventstore:23.10.0-jammy")
                .WithName($"{nameof(TestContainersService)}-container")
                .WithAutoRemove(true)
                .WithHostname("test-host")
                .WithCleanUp(true)
                .WithPortBinding(2113, 2113)
                .Build();

            if (EventSourceDbContainer != null)
            {
                await EventSourceDbContainer.StartAsync();
            }
        }

        public async Task StopAsync()
        {
            await EventSourceDbContainer.StopAsync();
        }

        public async ValueTask DisposeAsync()
        {
            if (EventSourceDbContainer != null)
            {
                await EventSourceDbContainer.DisposeAsync();
            }
        }
    }
}