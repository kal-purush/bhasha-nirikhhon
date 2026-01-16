using System;
using Microsoft.Extensions.DependencyInjection;
using SenseNet.Communication.Messaging;
using SenseNet.Messaging.RabbitMQ;
using SenseNet.Messaging.RabbitMQ.Configuration;

// ReSharper disable once CheckNamespace
namespace SenseNet.Extensions.DependencyInjection
{
    public static class RabbitMqExtensions
    {
        public static IServiceCollection AddRabbitMqMessageProvider(this IServiceCollection services,
            Action<ClusterMemberInfo> configureClusterMemberInfo = null, Action<RabbitMqOptions> configureRabbitMq = null)
        {
            services
                .AddSenseNetMessaging<RabbitMQMessageProvider>(configureClusterMemberInfo)
                .Configure<RabbitMqOptions>(opt => { configureRabbitMq?.Invoke(opt); });

            return services;
        }
    }
}