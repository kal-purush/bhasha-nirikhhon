using System;
using JetBrains.Annotations;

namespace Vostok.ClusterConfig.Client.Helpers
{
    internal static class DefaultClusterConfigClientProvider
    {
        private static readonly object Sync = new object();

        private static volatile ClusterConfigClient client;

        public static ClusterConfigClient Get()
        {
            if (client != null)
                // ReSharper disable once InconsistentlySynchronizedField
                return client;

            lock (Sync)
            {
                client = client ?? Create();
                return client;
            }
        }

        public static void Configure([NotNull] ClusterConfigClient newClient, bool canOverwrite = false)
        {
            if (newClient == null)
                throw new ArgumentNullException(nameof(newClient));

            lock (Sync)
            {
                if (!canOverwrite && client != null)
                    throw new InvalidOperationException("Can't overwrite existing ClusterConfigClient.");

                client = newClient;
            }
        }

        private static ClusterConfigClient Create() => new ClusterConfigClient();
    }
}