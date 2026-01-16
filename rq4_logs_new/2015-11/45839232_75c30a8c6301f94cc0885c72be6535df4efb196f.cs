using System;
using System.Collections.Generic;
using System.Net;
using Elders.Cronus.IocContainer;
using Elders.Cronus.Pipeline.Config;
using Elders.Cronus.Pipeline.Hosts;
using RedLock;

namespace Elders.Cronus.AtomicAction.Redis.Config
{
    public class RedisAggregateRootAtomicActionSettings : IRedisAggregateRootAtomicActionSettings
    {
        public RedisAggregateRootAtomicActionSettings()
        {
            this.SetLockClockDriveFactor(RedLockOptions.Default.ClockDriveFactor);
            this.SetLockRetryCount(RedLockOptions.Default.LockRetryCount);
            this.SetLockRetryDelay(RedLockOptions.Default.LockRetryDelay);
            this.SetLockTtl(RedisAtomicActionOptions.Defaults.LockTtl);
        }

        TimeSpan IRedisAggregateRootAtomicActionSettings.LockTtl { get; set; }

        IEnumerable<IPEndPoint> IRedisAggregateRootAtomicActionSettings.EndPoints { get; set; }

        double IRedisAggregateRootAtomicActionSettings.ClockDriveFactor { get; set; }

        int IRedisAggregateRootAtomicActionSettings.LockRetryCount { get; set; }

        TimeSpan IRedisAggregateRootAtomicActionSettings.LockRetryDelay { get; set; }
    }

    #region This will be deleted after updating the Cronus nuget package

    public interface IAggregateRootAtomicActionSettings : ISettingsBuilder
    {
        IAggregateRootAtomicAction AggregateRootAtomicAtion { get; set; }
    }

    public class AggregateRootAtomicActionSettings : SettingsBuilder, IAggregateRootAtomicActionSettings
    {

        public AggregateRootAtomicActionSettings(ISettingsBuilder settingsBuilder) : base(settingsBuilder)
        {
        }

        IAggregateRootAtomicAction IAggregateRootAtomicActionSettings.AggregateRootAtomicAtion { get; set; }

        public override void Build()
        {
            var builder = this as ISettingsBuilder;
            var casted = this as IAggregateRootAtomicActionSettings;
            builder.Container.RegisterSingleton<IAggregateRootAtomicAction>(() => casted.AggregateRootAtomicAtion, builder.Name);
        }
    }

    public interface IClusterSettings : ISettingsBuilder
    {
        string ClusterName { get; set; }
    }

    public class ClusterSettings : SettingsBuilder, IClusterSettings
    {
        public ClusterSettings(ISettingsBuilder settingsBuilder) : base(settingsBuilder)
        {
        }

        string IClusterSettings.ClusterName { get; set; }

        public override void Build()
        {
        }
    }

    public static class ClusterSettingExtensions
    {
        public static T UseCluster<T>(this T self, Action<ClusterSettings> configure = null) where T : ICronusSettings
        {
            var settings = new ClusterSettings(self);

            if (configure != null)
                configure(settings);

            (settings as ISettingsBuilder).Build();

            return self;
        }
    }

    #endregion
}