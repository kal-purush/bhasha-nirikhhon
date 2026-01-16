using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using System;
using System.Collections.Generic;

namespace StefaniniCore.API.HealthChecks
{
    internal static class SqlServerHealthCheck
    {
        /// <summary>
        /// Check the Health of SQL Server instance.
        /// </summary>
        /// <param name="builder"></param>
        /// <param name="name">Name field on JSON.</param>
        /// <param name="connectionString">Connection String.</param>
        /// <param name="failureStatus"></param>
        /// <param name="tags"></param>
        /// <param name="thresholdInBytes"></param>
        /// <returns></returns>
        public static IHealthChecksBuilder AddSQLServerInfo(
           this IHealthChecksBuilder builder,
           string name,
           string connectionString,
           HealthStatus? failureStatus = null,
           IEnumerable<string> tags = null,
           long? thresholdInBytes = null)
        {
            // Set the ConnectionString.
            builder.AddSqlServer(connectionString, null, name,
                              failureStatus ?? HealthStatus.Degraded, tags
                              , TimeSpan.FromSeconds(60));

            return builder;
        }
    }
}