using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using StackExchange.Redis;

namespace SFA.DAS.Tools.Servicebus.Support.Web.App_Start
{
    public static class DistributedCacheConfigurationExtension
    {
        private const string ApplicationName = "das-tools-service";
        public static void AddDistributedCache(this IServiceCollection services, IConfiguration configuration, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                services.AddDistributedMemoryCache();
                services.AddDataProtection()
                    .SetApplicationName(ApplicationName);
            }
            else
            {
                var redisConnectionString = configuration["RedisConnectionString"];

                services.AddStackExchangeRedisCache(options =>
                {
                    options.Configuration = $"{redisConnectionString},DefaultDatabase=1";
                });

                var redis = ConnectionMultiplexer.Connect($"{redisConnectionString},DefaultDatabase=0");
                services.AddDataProtection()
                    .SetApplicationName(ApplicationName)
                    .PersistKeysToStackExchangeRedis(redis, "DataProtection-Keys");
            }
        }
    }
}