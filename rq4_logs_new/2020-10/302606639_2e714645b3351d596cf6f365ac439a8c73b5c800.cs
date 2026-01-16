using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.TestingHost;
using PubSubTest.Abstractions;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace PubSubTest.Tests
{
    public class UnitTests
    {
        public sealed class Silo : IStartupTask, IDisposable
        {
            private static readonly List<Silo> allSilos = new List<Silo>();
            private readonly HashSet<string> received = new HashSet<string>();

            public IPubSub PubSub { get; }

            public static IReadOnlyCollection<Silo> All => allSilos;

            public IReadOnlyCollection<string> Received => received;

            public Silo(IPubSub pubSub)
            {
                PubSub = pubSub;
            }

            public static void Clear()
            {
                allSilos.Clear();
            }

            public Task Execute(CancellationToken cancellationToken)
            {
                lock (allSilos)
                {
                    allSilos.Add(this);
                }

                PubSub.Subscribe(message =>
                {
                    received.Add(message);
                });

                return Task.CompletedTask;
            }

            public void Dispose()
            {
                lock (allSilos)
                {
                    allSilos.Remove(this);
                }
            }
        }

        public class Configurator : ISiloConfigurator
        {
            public void Configure(ISiloBuilder siloBuilder)
            {
                siloBuilder.AddPubSub();

                siloBuilder.AddStartupTask<Silo>();
            }
        }

        public UnitTests()
        {
            Silo.Clear();
        }

        [Fact]
        public async Task Should_receive_pubsub_message()
        {
            var cluster =
                new TestClusterBuilder(3)
                    .AddSiloBuilderConfigurator<Configurator>()
                    .Build();

            await cluster.DeployAsync();

            await WaitForClusterSizeAsync(cluster, 3);

            await PublishAsync(3);
        }

        [Fact]
        public async Task Should_not_send_message_to_dead_member()
        {
            var cluster =
                new TestClusterBuilder(3)
                    .AddSiloBuilderConfigurator<Configurator>()
                    .Build();

            await cluster.DeployAsync();

            await cluster.KillSiloAsync(cluster.Silos[1]);

            await WaitForClusterSizeAsync(cluster, 2);

            await PublishAsync(2);

            await cluster.DisposeAsync();
        }

        [Fact]
        public async Task Should_send_message_to_new_member()
        {
            var cluster =
                new TestClusterBuilder(3)
                    .AddSiloBuilderConfigurator<Configurator>()
                    .Build();

            await cluster.DeployAsync();

            cluster.StartAdditionalSilo();

            await WaitForClusterSizeAsync(cluster, 4);

            await PublishAsync(4);

            await cluster.DisposeAsync();
        }

        private async Task PublishAsync(int expectedCount)
        {
            var message = Guid.NewGuid().ToString();

            await Silo.All.First().PubSub.PublishAsync(message);

            Assert.Equal(expectedCount, Silo.All.Count(x => x.Received.Contains(message)));
        }

        private async Task<bool> WaitForClusterSizeAsync(TestCluster cluster, int expectedSize)
        {
            var brokerGrain = cluster.Client.GetGrain<IPubSubBrokerGrain>(Constants.BrokerId);

            var timeout = TimeSpan.FromSeconds(10);

            var stopWatch = Stopwatch.StartNew();
            do
            {
                var hosts = await brokerGrain.GetClusterSizeAsync();

                if (hosts == expectedSize)
                {
                    stopWatch.Stop();
                    return true;
                }

                await Task.Delay(100);
            }
            while (stopWatch.Elapsed < timeout);

            return false;
        }
    }
}