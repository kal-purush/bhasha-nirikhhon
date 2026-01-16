using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.TestingHost;
using PubSubTest.OrleansStreams;
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace PubSubTest.Tests
{
    public class OrleansStreamingPubSubTests
    {
        private sealed class Configurator : ISiloConfigurator
        {
            public void Configure(ISiloBuilder siloBuilder)
            {
                siloBuilder.AddPubSub();
                siloBuilder.AddStartupTask<Silo>();
            }
        }

        public OrleansStreamingPubSubTests()
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

            await cluster.StopSiloAsync(cluster.Silos[1]);

            await WaitForClusterSizeAsync(cluster, 2);

            await PublishAsync(2);

            await cluster.DisposeAsync();
        }

        [Fact]
        public async Task Should_not_send_message_to_dead_but_not_unregistered_member()
        {
            var cluster =
                new TestClusterBuilder(3)
                    .AddSiloBuilderConfigurator<Configurator>()
                    .Build();

            await cluster.DeployAsync();

            await cluster.KillSiloAsync(cluster.Silos[1]);

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
            var managementGrain = cluster.Client.GetGrain<IManagementGrain>(0);

            var timeout = TestCluster.GetLivenessStabilizationTime(new ClusterMembershipOptions());

            var stopWatch = Stopwatch.StartNew();
            do
            {
                var hosts = await managementGrain.GetHosts();

                if (hosts.Count == expectedSize)
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