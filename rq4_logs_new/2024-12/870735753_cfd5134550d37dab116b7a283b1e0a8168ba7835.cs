using StackExchange.Redis;
using Store.G01.Core.ServicesContract;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Store.G01.Service.Serveices.Caches
{
	public class CacheService : IcacheService
	{
		private readonly IDatabase _database;
        public CacheService(IConnectionMultiplexer redis)
        {
			_database=redis.GetDatabase();
        }
        public async Task<string> GetCasheKeyAsync(string key)
		{
			var cacheResponse= await _database.StringGetAsync(key);
			if (cacheResponse.IsNullOrEmpty) return null;
			return cacheResponse.ToString();
		}

		public async Task SetCasheKeyAsync(string Key, object response, TimeSpan expireTime)
		{
			if(response == null) return;

			var options= new JsonSerializerOptions() { PropertyNamingPolicy= JsonNamingPolicy.CamelCase };

			await _database.StringSetAsync(Key, JsonSerializer.Serialize(response,options), expireTime);
		}
	}
}