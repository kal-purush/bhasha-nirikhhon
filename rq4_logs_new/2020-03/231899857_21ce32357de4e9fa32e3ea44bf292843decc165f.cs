using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

using HttpServices.Interfaces;
using HttpServices.CacheDecorators;

namespace Aggregator.ServicesConfiguration
{
	internal static class CacheConfiguration
	{
		public static void AddCache(this IServiceCollection services, IConfiguration configuration)
		{
			services.AddDistributedMemoryCache();
			services.AddStackExchangeRedisCache(options =>
			{
				options.Configuration = configuration.GetValue<string>("Redis:ConnectionString");
			});
		}

		public static void RegisterCacheServices(this IServiceCollection services, IConfiguration configuration)
		{
			services.Decorate<IPhotosService, PhotosCacheService>();
		}
	}
}