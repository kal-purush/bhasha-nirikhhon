using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.TestingHost;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace PubSubTest.Tests
{
    public sealed class OrleansSimpleStreamingPubSubTests : IAsyncLifetime
    {
        public OrleansSimpleStreamingPubSubTests()
        {
            TestContext.ServiceProviders.Value = new ConcurrentDictionary<IServiceProvider, bool>();

            _key = Guid.NewGuid().ToString();
            _cluster = new TestClusterBuilder(3)
                    .AddSiloBuilderConfigurator<SiloConfigurator>()
                    .Build();
        }

        private readonly string _key;
        private readonly TestCluster _cluster;

        public async Task InitializeAsync()
        {
            await _cluster.DeployAsync();
        }

        public async Task DisposeAsync()
        {
            await _cluster.DisposeAsync();
        }

        [Fact]
        public async Task Should_receive_pubsub_message()
        {
            await PublishAsync(3, _key);
        }

        [Fact]
        public async Task Should_not_send_message_to_dead_member()
        {
            await _cluster.StopSiloAsync(_cluster.Silos[1]);
            await PublishAsync(2, _key);
        }

        [Fact]
        public async Task Should_not_send_message_to_dead_but_not_unregistered_member()
        {
            await _cluster.KillSiloAsync(_cluster.Silos[1]);
            await PublishAsync(2, _key);
        }

        [Fact]
        public async Task Should_send_message_to_new_member()
        {
            await _cluster.StartAdditionalSiloAsync();
            await PublishAsync(4, _key);
        }

        private async Task PublishAsync(int expectedCount, string key)
        {
            var message = Guid.NewGuid().ToString();

            // wake up each local replica
            foreach (var provider in TestContext.ServiceProviders.Value.Keys)
            {
                var silo = provider.GetRequiredService<Silo>();
                await provider.GetRequiredService<IGrainFactory>().GetSimpleStreamingReplicaGrain(silo.SiloAddress, key).GetAsync();
            }

            // push the message
            await TestContext.ServiceProviders.Value.Keys.First()
                .GetRequiredService<IGrainFactory>()
                .GetStreamingPublisherGrain(key)
                .SendAsync(message);

            // get the propagated message
            var actual = 0;
            foreach (var provider in TestContext.ServiceProviders.Value.Keys)
            {
                var silo = provider.GetRequiredService<Silo>();
                var result = await provider.GetRequiredService<IGrainFactory>().GetSimpleStreamingReplicaGrain(silo.SiloAddress, key).GetAsync();
                actual += result == message ? 1 : 0;
            }

            Assert.Equal(expectedCount, actual);
        }
    }

    public static class TestContext
    {
        public static AsyncLocal<ConcurrentDictionary<IServiceProvider, bool>> ServiceProviders { get; } = new AsyncLocal<ConcurrentDictionary<IServiceProvider, bool>>();
    }

    public class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddSimpleMessageStreamProvider("SMS2");
            siloBuilder.AddMemoryGrainStorageAsDefault();
            siloBuilder.AddMemoryGrainStorage("PubSubStore");
            siloBuilder.AddStartupTask((provider, token) =>
            {
                // expose the silo service providers to the running test
                if (TestContext.ServiceProviders != null)
                {
                    TestContext.ServiceProviders.Value.TryAdd(provider, true);

                    provider.GetRequiredService<Silo>().SiloTerminated.ContinueWith(x =>
                    {
                        TestContext.ServiceProviders.Value.TryRemove(provider, out _);
                    });
                }

                return Task.CompletedTask;
            });
        }
    }
}